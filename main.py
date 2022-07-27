# This is a Praktični zadatak – By Omar Hassan
from multiprocessing import connection
import requests;
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import psycopg2

# api variables
api_key= "294161a54e075b1361e06d7f6ea9aeff"
api_url = "https://api.themoviedb.org/3"
p_movies_path = f"/movie/popular?api_key={api_key}"
genres_path = "/genre/movie/list?api_key=294161a54e075b1361e06d7f6ea9aeff&language=en-US"
credits_path = "/credits?api_key=294161a54e075b1361e06d7f6ea9aeff"
reviews_path = "/reviews?api_key=294161a54e075b1361e06d7f6ea9aeff&language=en-US&page=1"

# database connection
hostname = 'tmdb'
database= 'exampledb'
username= 'docker'
pwd ='docker'
port_id = '5432'
conn = None
cur = None


try:
    conn = psycopg2.connect(
        host = hostname,
        dbname = database,
        user = username,
        password = pwd,
        port = port_id
    )

    cur = conn.cursor()

    #create main table
    create_script = ''' CREATE TABLE IF NOT EXISTS popular_movies(
                            id int PRIMARY KEY,
                            original_title varchar(100) NOT NULL,
                            genre varchar(50) ARRAY,
                            release_date date,
                            cast_ids varchar(50) ARRAY,
                            director_id int,
                            senitment_fedback varchar(50),
                            senitment_analysis varchar(50)
    ) '''
    cur.execute(create_script)

    # create crew table
    create_crew = ''' CREATE TABLE IF NOT EXISTS crew(
                            id int PRIMARY KEY,
                            adult varchar(50),
                            gender int,
                            known_for_department varchar(50),
                            _name_ varchar(50),
                            original_name varchar(50),
                            popularity decimal,
                            profile_path varchar(100),
                            credit_id varchar(100),
                            department varchar(50),
                            job varchar(50)                       
    ) '''
    cur.execute(create_crew)

    # create cast table
    create_cast = ''' CREATE TABLE IF NOT EXISTS cast_(
                            id int PRIMARY KEY,
                            adult varchar(50),
                            gender int,
                            known_for_department varchar(50),
                            _name_ varchar(50),
                            original_name varchar(50),
                            popularity decimal,
                            profile_path varchar(100),
                            cast_id int,
                            _character_ varchar(200),
                            credit_id varchar(100),
                            _order_ int                     
    ) '''
    cur.execute(create_cast)
    delete_script = 'DELETE FROM popular_movies '
    delete_script2 = 'DELETE FROM crew'
    delete_script3 = 'DELETE FROM cast_'

    #if there is data in the tables clear it
    cur.execute(delete_script)
    cur.execute(delete_script2)
    cur.execute(delete_script3)
    
    # senitment analysis
    def sentiment_vader(sentence):
        # Create a SentimentIntensityAnalyzer object.
        sid_obj = SentimentIntensityAnalyzer()

        sentiment_dict = sid_obj.polarity_scores(sentence)
        negative = sentiment_dict['neg']
        neutral = sentiment_dict['neu']
        positive = sentiment_dict['pos']
        compound = sentiment_dict['compound']

        if sentiment_dict['compound'] >= 0.05:
            overall_sentiment = "Positive"

        elif sentiment_dict['compound'] <= - 0.05:
            overall_sentiment = "Negative"

        else:
            overall_sentiment = "Neutral"

        return negative, neutral, positive, compound, overall_sentiment


    #get genres id and names for later use
    genres_r = requests.get(f"{api_url}{genres_path}")
    if genres_r.status_code in range(200,299):
        genres_data = genres_r.json()
        g = genres_data["genres"]



    pm_endpoint = f"{api_url}{p_movies_path}"
    r = requests.get(pm_endpoint)
    if r.status_code in range(200,299):
        data = r.json()
        results = data['results']
        if len(results) > 0:
            for result in results:
                genre_array=[]
                cast_ids_array = []
                #grab the genre IDs and
                for genre in result['genre_ids']:
                    #finds genre name by id
                    genres_l=list(filter(lambda item: item['id'] == genre, g))
                    genre_array.append(genres_l[0].get("name"))

                credits_r = requests.get(f"{api_url}/movie/{result['id']}{credits_path}")
                if credits_r.status_code in range(200, 299):
                    credits_data = credits_r.json()
                    cast_d = credits_data["cast"]

                    # loop throw cast data and insert it in cast table
                    for cast in cast_d:
                        insert_cast_script = "INSERT INTO cast_ (adult,gender,id,known_for_department,_name_,original_name,popularity" \
                                             ",profile_path,cast_id,credit_id,_character_ ,_order_ ) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"
                        insert_cast = (cast.get('adult'), cast.get('gender'), cast.get('id'),
                                       cast.get('known_for_department'),
                                       cast.get('name'), cast.get('original_name'),
                                       cast.get('popularity'), cast.get('profile_path'),
                                       cast.get('cast_id'),cast.get('credit_id'), cast.get('character'), cast.get('order'))
                        cur.execute(insert_cast_script, insert_cast)
                        cast_ids_array.append(cast.get('id'))



                    # loop throw cast data and insert it in cast table
                    crew_d = credits_data["crew"]
                    crew_l = list(filter(lambda item: item['job'] == "Director", crew_d))

                    insert_crew_script = "INSERT INTO crew (adult,gender,id,known_for_department,_name_,original_name,popularity" \
                                         ",profile_path,credit_id,department,job) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"
                    insert_crew= (crew_l[0].get('adult'),crew_l[0].get('gender'),crew_l[0].get('id'),crew_l[0].get('known_for_department'),
                                crew_l[0].get('name'),crew_l[0].get('original_name'),crew_l[0].get('popularity'),crew_l[0].get('profile_path')
                                ,crew_l[0].get('credit_id'),crew_l[0].get('department'),crew_l[0].get('job'))
                    cur.execute(insert_crew_script, insert_crew)



                reviews_r = requests.get(f"{api_url}/movie/{result['id']}{reviews_path}")
                if reviews_r.status_code in range(200, 299):
                    reviews_data = reviews_r.json()
                    r_results = reviews_data["results"]
                    if(len(r_results)>0):

                        
                        positive=0
                        negative=0
                        for content in r_results:
                            if(sentiment_vader(content['content'])[4]=="Positive"):
                                positive+=1
                            else:
                                negative += 1
                            
                            sentiment_results=f"{positive} positive reviews\n{negative} negative reviews"
                            if(positive>negative):
                                sentiment_general="Positive"
                            elif(negative>positive):
                                sentiment_general="Negative"
                            else:
                                sentiment_general = "Neutral"

                    else:
                        sentiment_result="No reviews"

                insert_script="INSERT INTO popular_movies (id, original_title,genre,release_date,cast_ids,"\
                "director_id,senitment_fedback,senitment_analysis)VALUES(%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING"
                insert_values =(result['id'], result['original_title'],genre_array,result['release_date'],
                                cast_ids_array,crew_l[0].get('id'),sentiment_general,sentiment_results)
                cur.execute(insert_script,insert_values)

            #  commit changes to db   
            conn.commit()



except Exception as error:
    print(error)

finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()



