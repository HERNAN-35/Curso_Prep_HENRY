# Librerias
import pandas as pd
import numpy as np
import json
import ast
from fastapi import FastAPI
import uvicorn


# Datasets
df_f1yf2 = pd.read_parquet("Datasets\df_f1yf2.parquet")
df_f3f4yf5 = pd.read_parquet("Datasets\df_f3f4yf5.parquet")

app = FastAPI()

# Endpoint_1_2_3_4_5_6

# Endpoint_1
@app.get("/PlayTimeGenre/{genre}")
def PlayTimeGenre(genre):
    # Columnas a utilizar:
    df_1 = df_f1yf2[["genres", "año_lanzamiento", "playtime_forever"]] 
    # Funcion: 
    if type(genre) == str:
        ngenre = genre.capitalize()
        mask = df_1['genres'].apply(lambda x: ngenre in x)
        df_f1_by_genre = df_1[mask]
        grouped_df_f1_by_year = df_f1_by_genre.groupby('año_lanzamiento')['playtime_forever'].sum().reset_index()
        df1genre = grouped_df_f1_by_year[['año_lanzamiento','playtime_forever']]
        df1genre.sort_values(by='playtime_forever', ascending=False, inplace=True)
        df1genre.reset_index(drop=True, inplace=True)
        year_most_hours_played = df1genre.iloc[0,0]
        F1message = {f"Release year with the most played hours for Genre {ngenre}": int(year_most_hours_played)}
        json_message = json.dumps(F1message, indent=4)
        return print(json_message)

# Endpoint_2
@app.get("/UseForGenre/{genre}")
def UseForGenre(genre):
    # Columnas a utilizar:
    df_2 = df_f1yf2[["user_id","genres","año_lanzamiento","playtime_forever"]]
    # Funcion:
    ug = set()
    for genres_lista in df_2['genres']:
        ug.update(genres_lista)
    lista_unica_genres = list(ug)
    def normalizar(lista_generos):
        x = []
        for sentence in lista_generos:
            xxxxs = sentence.split()  
            xx = [xxxx.capitalize() for xxxx in xxxxs]  
            xxx = ' '.join(xx)  
            x.append(xxx)
        return x 
    normalizado_genres = normalizar(lista_unica_genres)
    def capitalize_first_words_in_sentence(sentence):
        words = sentence.split()  
        capitalized_words = [word[0].capitalize() + word.lower()[1:] for word in words]  
        return ' '.join(capitalized_words)
    if type(genre) == str:
        norm_genre = capitalize_first_words_in_sentence(genre)
        if norm_genre in normalizado_genres:
            genre_to_find = norm_genre
            if norm_genre == 'Free To Play':
                genre_to_find = 'Free to Play'
            elif norm_genre == 'Rpg':
                genre_to_find = 'RPG'
            mask = df_2['genres'].apply(lambda x: genre_to_find in x)
            df_f2_by_genre = df_2[mask]
            grouped_df_f2_by_user_year = df_f2_by_genre.groupby(['user_id','año_lanzamiento'])['playtime_forever'].sum().reset_index()
            grouped_df_f2_by_user = df_f2_by_genre.groupby(['user_id'])['playtime_forever'].sum().reset_index()
            grouped_df_f2_by_user.sort_values(by='playtime_forever', ascending=False, inplace=True)
            grouped_df_f2_by_user.reset_index(drop=True, inplace=True)
            user_most_hours_played = grouped_df_f2_by_user.iloc[0,0]
            mask = grouped_df_f2_by_user_year['user_id'] == user_most_hours_played
            resultF2 = grouped_df_f2_by_user_year[mask]
            playtime_list = resultF2.rename(columns={'año_lanzamiento': 'Release Year', 'playtime_forever': 'Hours'})[['Release Year', 'Hours']].to_dict(orient='records')
            F2message = {
                f"User with the most played hours for Genre {genre_to_find}": user_most_hours_played,
                "Playtime": playtime_list}
            json_message = json.dumps(F2message, indent=4)
            return print(json_message)
        else:
            return print("Insert a valid genre")
    else:
        return print("Insert a valid genre")

# Endpoint_3
@app.get("/UsersRecommend/{year}")
def UsersRecommend(year):
    # Columnas a utilizar:
    df_3 = df_f3f4yf5[["item_id","title","recommend","analisis_sentimiento","año_publicado"]]
    # Funcion:
    if type(year) == int:
        mask = df_3['año_publicado'] == year
        df_f3_review_year = df_3[mask].reset_index(drop=True)
        grouped_df_f3_review_year = df_f3_review_year.groupby(['title'])['analisis_sentimiento'].sum().reset_index()
        grouped_df_f3_review_year.sort_values(by='analisis_sentimiento', ascending=False, inplace=True)
        if not grouped_df_f3_review_year.empty:
            item_Rank_1 = grouped_df_f3_review_year.iloc[0,0]
            item_Rank_2 = grouped_df_f3_review_year.iloc[1,0]
            item_Rank_3 = grouped_df_f3_review_year.iloc[2,0]
            dataF3 = {
                'Rank': ['Position 1', 'Position 2', 'Position 3'],
                'title': [item_Rank_1, item_Rank_2, item_Rank_3]}
            json_data = [{"Rank " + str(index + 1): item} for index, item in enumerate(dataF3['title'])]
            json_message = json.dumps(json_data, indent=None, ensure_ascii=False)
            return print(json_message)
        else:
            return print("There are no reviews for the year entered.")
    else:
        return print("There are no reviews for the year entered.")

# Endpoint_4
@app.get("/UsersNotRecommend/{year}")
def UsersNotRecommend(year):
    # Columnas a utilizar:
    df_4 = df_f3f4yf5[["item_id","title","recommend","analisis_sentimiento","año_publicado"]]
    # Funcion:
    if type(year) == int:
        mask = df_4['año_publicado'] == year
        df_f4_review_year = df_4[mask].reset_index(drop=True)
        if not df_f4_review_year.empty:
            counter = df_f4_review_year['title'].value_counts()
            top_3_least_recommended = counter.head(3)
            json_data = [{"Rank " + str(index + 1): item} for index, item in enumerate(top_3_least_recommended.index)]
            json_message = json.dumps(json_data, indent=None, ensure_ascii=False)
            return print(json_message)
        else:
            return print("There are no reviews for the year entered.")
    else:
        return print("There are no reviews for the year entered.")

# Endpoint_5
@app.get("/Sentiment_analysis/{year}")
def Sentiment_analysis(year):
    # Columnas a utilizar:
    df_5 = df_f3f4yf5[["analisis_sentimiento","año_lanzamiento"]]
    # Funcion:
    if type(year) == int:
        mask = df_5['año_lanzamiento'] == year
        df_f5_review_year = df_5[mask].reset_index(drop=True)
        if not df_f5_review_year.empty:
            counter = df_f5_review_year['analisis_sentimiento'].value_counts().sort_index()
            dataF5 = {
                "Negative": int(counter.get(0, 0)),
                "Neutral": int(counter.get(1, 0)),
                "Positive": int(counter.get(2, 0))}
            json_message = json.dumps(dataF5, indent=None, ensure_ascii=False)

            return print(json_message)
        else:
            return print("The year entered does not have a record to perform the sentiment analysis")
    else:
        return print("The year entered does not have a record to perform the sentiment analysis")

# Endpoint_6





