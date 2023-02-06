from bs4 import BeautifulSoup
import datetime as dt
import pandas as pd
import requests
import concurrent.futures

MAX_THREADS = 30

def get_game_list():
    df = pd.read_csv("https://raw.githubusercontent.com/petebrown/update-player-stats/main/data/players_df.csv")

    df.sb_game_id = df.sb_game_id.str.replace("tpg", "")

    df["url"] = df.apply(lambda x: f"https://www.soccerbase.com/matches/additional_information.sd?id_game={x.sb_game_id}", axis=1)

    games = df[["url", "venue"]].drop_duplicates().to_dict("records")

    return games

def make_record(game_id, venue, player_id, event):
    record = {
        "game_id": game_id,
        "venue": venue,
        "player_id": player_id,
        "event_details": event.text.strip(),
        }
    return record

def get_player_id(sub_text):
    player_id = sub_text.find("a")["href"].split("=")[1]
    return player_id

def get_events(doc, venue, game_id, side):
    match_events = []

    subs_off = doc.select(f'.lineup .{side} .firstTeam .replaced')
    subs_on = doc.select(f'.lineup .{side} .reserve tr:not(.replaced)')
    dub_subs = doc.select(f'.lineup .{side} .reserve tr')
    red_cards = doc.select(f'.lineup .{side} .sendingOff')

    for sub in subs_off:
        player_id = get_player_id(sub)
        record = make_record(game_id, venue, player_id, sub)
        match_events.append(record)

    for sub in subs_on:
        player_id = get_player_id(sub)
        record = make_record(game_id, venue, player_id, sub)
        match_events.append(record)

    for sub in dub_subs:
        if '-' in sub.text:
            player_id = get_player_id(sub)

            record_1 = make_record(game_id, venue, player_id, sub)
            record_2 = make_record(game_id, venue, player_id, sub)

            match_events.append(record_1)
            match_events.append(record_2)
        else:
            next

    for red in red_cards:
        player_id = get_player_id(red)

        record = make_record(game_id, venue, player_id, red)
        
        match_events.append(record)
    return match_events

def scrape_events(game_dict):
    url = game_dict["url"]
    venue = game_dict["venue"]
    game_id = url.split("=")[1]

    if venue == "H":
        side = "teamA"
    elif venue == "A":
        side = "teamB"
    else:
        next

    r = requests.get(url)
    doc = BeautifulSoup(r.text, 'html.parser')

    events = get_events(doc, venue, game_id, side)
    return events

def async_scraping(scrape_function, urls):
    threads = min(MAX_THREADS, len(urls))
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        results = executor.map(scrape_function, urls)

    return results

def get_events_df():
    games = get_game_list()

    df = async_scraping(scrape_events, games)
    df = list(df)
    df = [sub for sublist in df for sub in sublist]
    df = pd.DataFrame(df)
    
    df = df[(df.event_details.str.contains("\(\d+-\d+\)", regex=True)) | (df.event_details.str.contains("\(\d+", regex=True)) | (df.event_details.str.contains("s/o"))].copy()
    df["min_on"] = df.event_details.str.extract(r"\((\d+)")
    df["min_off"] = df.event_details.str.extract(r"\-(\d+)\)")
    df["min_so"] = df.event_details.str.extract(r"s/o (\d+)\)")
    df = df[["game_id", "player_id", "min_on", "min_off", "min_so", "event_details"]].drop_duplicates().reset_index(drop=True)
    return df

df = get_events_df()
df.to_csv("./data/subs-and-reds.csv", index=False)