import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import re
import json
from datetime import datetime
import pickle
import time


r = requests.get('https://www.letour.fr/en/history')
soup = BeautifulSoup(r.text, 'html.parser')
results = soup.find_all('button') #class="dateTabs__link js-tabs"')


year_links_dict = {}

for item in results:
    if item.text:
        key = int(item.text)
        value = {'mainurl':item['data-tabs-ajax']}
        year_links_dict[key] = value
        

#### Adding links to `Starters`, `Stages`, `Jersey wearers`, `Stage winners` and `Ranking` 
base_url = 'https://www.letour.fr'

for year in list(year_links_dict.keys()):
    print('Main URL: ', year, year_links_dict[year]['mainurl'])
    r = requests.get(base_url + year_links_dict[year]['mainurl'])
    soup = BeautifulSoup(r.text, 'html.parser')
    
    new_dict = {}
    buttons = soup.find_all('button', class_="js-tabs-nested")
    for b in buttons:
        new_key = b.text.lower().replace(' ', '_') + '_url'
        new_val = b['data-tabs-ajax']
        year_links_dict[year][new_key] = new_val


# Save to `pickle`
def save_pickle(obj, filename):
    with open('data/' + filename + '.pkl', 'wb') as f:
        pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

with open('data/year_links_dict.pkl', 'wb') as f:
    pickle.dump(year_links_dict, f, pickle.HIGHEST_PROTOCOL)


# Open from `pickle`
def load_pickle(name):
    with open('data/' + name + '.pkl', 'rb') as f:
        return pickle.load(f)

year_links_dict = load_pickle('year_links_dict')
year_links_dict


#### Add `num_of_stages` to dict

# Function to scrape the total number of stages, for a particular year
base_url = 'https://www.letour.fr'

def scrape_number_of_stages(year):
    print(f'Scraping number of stages for {year}')
    r = requests.get(base_url + year_links_num_dict[year]['mainurl'])
    soup = BeautifulSoup(r.text, 'html.parser')
    return int(soup.find_all('span', class_='statsInfos__number')[0].text)

year_links_num_dict = year_links_dict.copy()

for year in list(year_links_num_dict.keys()):
    year_links_num_dict[year]['num_of_stages'] = scrape_number_of_stages(year)
save_pickle(year_links_num_dict, 'year_links_num_dict')


### Load `year_links_num_dict`
year_links_num_dict = load_pickle('year_links_num_dict')
year_links_num_dict


# #### 1. Starters
base_url = 'https://www.letour.fr'

def scrape_starters(year):
    print(f'Scraping starters for {year}')
    r = requests.get(base_url + year_links_num_dict[year]['starters_url'], timeout=10)
    soup = BeautifulSoup(r.text, 'html.parser')
    
    rows = soup.find_all('tr')
    rows_list = []

    for i, row in enumerate(rows):
        rows_list.append(row.text.strip())
        
    starters_list = []

    for row in rows_list:
        matched_on_team = re.search(r'^[a-zA-Z]', row)

        if matched_on_team: # if row is a team name
            new_row = [row,None,None]
            starters_list.append(new_row)
        else: # row is a rider
            new_row = [None]
            new_row.append([x.strip() for x in row.split('\n ')][0])
            new_row.append([x.strip() for x in row.split('\n ')][1])
            starters_list.append(new_row)
            
    starters_df = pd.DataFrame(starters_list, columns=['team','rider_num','rider_name'])
    starters_df.team = starters_df.team.fillna(method='ffill')
    starters_df = starters_df[starters_df.rider_num.isnull() == False]
    starters_df = starters_df.reset_index(drop=True)
    filepath = 'data/' + str(year) + '/' + str(year) + '_starters' + '.csv'
    starters_df.to_csv(filepath, index=False)
    print('Saved ' + filepath)    


# #### 2. Stages
base_url = 'https://www.letour.fr'

def scrape_stages(year):
    print(f'Scraping stages for {year}')
    r = requests.get(base_url + year_links_num_dict[year]['stages_url'], timeout=10)
    soup = BeautifulSoup(r.text, 'html.parser')
    
    stages_list = []
    rows = soup.find_all('tr')
    for i, row in enumerate(rows):
        stages_list.append(row.text.strip())

    stages_list = [i.split('\n') for i in stages_list]
    stages_list.pop(0)
    header = ['stage_num','date_start','start_city','finish_city']
    stages_df = pd.DataFrame(stages_list, columns=header)
    filepath = 'data/' + str(year) + '/' + str(year) + '_stages' + '.csv'
    stages_df.to_csv(filepath, index=False)
    print('Saved ' + filepath)    


# #### 3. Jersey wearers
base_url = 'https://www.letour.fr'

def scrape_jersey_wearers(year):
    print(f'Scraping jersey wearers for {year}')
    r = requests.get(base_url + year_links_num_dict[year]['jersey_wearers_url'], timeout=10)
    soup = BeautifulSoup(r.text, 'html.parser')
    jersey_list = []

    rows = soup.find_all('tr')
    for row in rows:
        new_row = row.text.strip('\n')
        new_row = re.sub(r'\s\s+',',',new_row)
        new_row = re.sub(r',$','',new_row)
        jersey_list.append(new_row)

    jersey_list = [i.split(',') for i in jersey_list]
    jersey_list.pop(0)
    header = ['stage_num','yellow_jersey','green_jersey','polka_dot_jersey','white_jersey']
    jersey_df = pd.DataFrame(jersey_list, columns=header)
    filepath = 'data/' + str(year) + '/' + str(year) + '_jersey_wearers' + '.csv'
    jersey_df.to_csv(filepath, index=False)
    print('Saved ' + filepath)    


# #### 4. Stage winners
base_url = 'https://www.letour.fr'

def scrape_stage_winners(year):
    print(f'Scraping stage winners for {year}')
    r = requests.get(base_url + year_links_num_dict[year]['stages_winners_url'], timeout=10)
    soup = BeautifulSoup(r.text, 'html.parser')
    stage_winners_list = []

    rows = soup.find_all('tr')
    for row in rows:
        new_row = row.text.strip('\n')
        new_row = re.sub(r'\s\s\s+',',',new_row)
        new_row = re.sub(r',$','',new_row)
        new_row = re.sub(r'\n',',',new_row)
        stage_winners_list.append(new_row)

    stage_winners_list = [i.split(',') for i in stage_winners_list]
    stage_winners_list.pop(0)
    header = ['stage_num','parcours','winner','team']
    stage_winners_df = pd.DataFrame(stage_winners_list
                                    , columns=header
                                   )
    filepath = 'data/' + str(year) + '/' + str(year) + '_stage_winners' + '.csv'
    stage_winners_df.to_csv(filepath, index=False)
    print('Saved ' + filepath)


# #### 5. Rankings per stage (i.e. times, gaps, points, etc.)
ranking_cats = {'indiv_general':'itg',
                'indiv_stage':'ite',
                'points_general':'ipg',
                #'points_stage':'ipe',
                'climber_general':'img',
                #'climber_stage':'ime',
                'youth_general':'ijg',
                #'combative_general':'icg',
                'team_stage':'ete',
                'team_general':'etg'
               }

base_url = 'https://www.letour.fr'

def scrape_all_rankings(year):
    print(f'Scraping rankings for all codes for {year}')
    
    for label, code in ranking_cats.items(): # loop through ranking codes
    
        print(label, code)

        num_columns = -1
        if code == 'itg' or code == 'ite':
#             continue
            header = ['rank','rider','rider_no','team','times','gap','b','p']
            num_columns = 8
        elif code == 'ipg':
#             continue
            header = ['rank','rider','rider_no','team','points','b','p']
            num_columns = 7
        elif code == 'img':
            header = ['rank','rider','rider_no','team','points']
            num_columns = 5
        elif code == 'ijg':
#             continue
            header = ['rank','rider','rider_no','team','times','gap']
            num_columns = 6
        elif code == 'ete' or code == 'etg':
#             continue
            header = ['rank','team','times','gap']
            num_columns = 4
        else:
            print('Not a ranking code I am interested in!')

        rankings_df = pd.DataFrame()

        for stage_num in range(year_links_num_dict[year]['num_of_stages']): # loop through stages
            stage_num += 1
            print('\nStage number:', stage_num)
            full_url = str(base_url + year_links_num_dict[year]['ranking_url'] 
                         + f"?stage={stage_num}" 
                         + f"&type={code}")
            r = requests.get(full_url, timeout=None)
            soup = BeautifulSoup(r.text, 'html.parser')
            print(full_url)

            rows_for_df = {} 
            row_num = 0
            for item in soup.tbody.find_all('tr'):
                row = item.find_all('td')

                if len(row) == num_columns: # key check!

                    new_row = []
                    for col in row: 
                        new_row.append(col.text.strip())
                        rows_for_df[row_num] = new_row
                    row_num += 1
               
            df = pd.DataFrame.from_dict(rows_for_df, orient='index'
                                    , columns=header
                                       )

            df['stage_num'] = stage_num
            cols = list(df.columns)
            cols = [cols[-1]] + cols[:-1]
            df = df[cols]
            df = df.reset_index(drop=True)

            rankings_df = pd.concat([rankings_df, df])
                                 
        filepath = 'data/' + str(year) + '/' + str(year) + '_rankings_' + code + '.csv'
        rankings_df.to_csv(filepath, index=False)
        print('Saved ' + filepath)
        time.sleep(5)


# ## Time to Scrape! Loop through all years...
ranking_cats = {'indiv_general':'itg',
                'indiv_stage':'ite',
                'points_general':'ipg',
                #'points_stage':'ipe',
                'climber_general':'img',
                #'climber_stage':'ime',
                'youth_general':'ijg',
                #'combative_general':'icg',
                'team_stage':'ete',
                'team_general':'etg'
               }

base_url = 'https://www.letour.fr'

for year in [2010,2011,2012]:
    directory = 'data/' + str(year)
    if not os.path.exists(directory):
        os.makedirs(directory)

    scrape_all_rankings(year) # 1. scrape rankings data
    time.sleep(10)
    scrape_starters(year) # 2. scrape starters data
    time.sleep(10)
    scrape_stages(year) # 3. scrape stages data
    time.sleep(10)
    scrape_jersey_wearers(year) # 4. scrape jersey wearers data
    time.sleep(10)
    scrape_stage_winners(year) # 5. scrape stage winners data
