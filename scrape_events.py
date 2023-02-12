from bs4 import BeautifulSoup
import pandas as pd
import requests
import concurrent.futures
import re

def get_game_list():
    df = pd.read_csv("https://raw.githubusercontent.com/petebrown/update-player-stats/main/data/players_df.csv")
    df.sb_game_id = df.sb_game_id.str.replace("tpg", "")
    df["url"] = df.apply(lambda x: f"https://www.soccerbase.com/matches/additional_information.sd?id_game={x.sb_game_id}", axis=1)

    games = df[["url", "venue"]].drop_duplicates().to_dict("records")

    return games

def create_record(game_id, player_id, min_off, min_so, min_on):
    record = {
        "game_id": game_id,
        "player_id": player_id,
        "min_on": min_on,
        "min_off": min_off,
        "min_so": min_so
        }
    return record

def get_player_id(event_text):
    player_id = event_text.find("a")["href"].split("=")[1]
    return player_id

def scrape_starter_events(event):
    regex = r"\((\d+)\)|\(s\/o (\d+)\)"
    matches = re.search(regex, event)
    min_off = matches.group(1)
    min_so = matches.group(2)
    return [min_off, min_so]

def scrape_sub_events(event):
    regex = r"\((\d+)-?(\d+)?(?:, s\/o )?(\d+)?\)"
    matches = re.search(regex, event)
    if matches:
        min_on = matches.group(1)
        min_off = matches.group(2)
        min_so = matches.group(3)
        return [min_on, min_off, min_so]

def get_events(doc, game_id, side):
    match_events = []

    subs_off = doc.select(f'.lineup .{side} .firstTeam .replaced')
    red_cards = doc.select(f'.lineup .{side} .firstTeam .sendingOff')

    starter_events = subs_off + red_cards
    sub_events = doc.select(f'.lineup .{side} .reserve tr')

    for event in starter_events:
        player_id = get_player_id(event)
        events = scrape_starter_events(event.text)
        
        min_off = events[0]
        min_so = events[1]
        
        record = create_record(game_id, player_id, min_off, min_so, min_on = None)
        
        match_events.append(record)

    for event in sub_events:
        regex = r"\((\d+)-?(\d+)?(?:, s\/o )?(\d+)?\)"
        matches = re.search(regex, event.text)
        if matches:
            player_id = get_player_id(event)
            events = scrape_sub_events(event.text)
        
            min_on = events[0]
            min_off = events[1]
            min_so = events[2]
            
            record = create_record(game_id, player_id, min_off, min_so, min_on)
        
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

    events = get_events(doc, game_id, side)
    return events

def async_scraping(scrape_function, urls):
    MAX_THREADS = 30

    threads = min(MAX_THREADS, len(urls))
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
        results = executor.map(scrape_function, urls)

    return results

def clean_events_list(input_list):
    events = list(input_list)
    events = [event for sublist in events for event in sublist]
    return events

def main():
    games = get_game_list()

    events = async_scraping(scrape_events, games)
    events = clean_events_list(events)
    
    df = pd.DataFrame(events)
    
    return df

df = main()
df.to_csv("./data/subs-and-reds.csv", index=False)