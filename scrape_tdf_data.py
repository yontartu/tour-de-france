# script to scrape tour de france data 

import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import os
import re
import json
from datetime import datetime
import pickle
import time

##### functions to work with pickle files #####

def save_pickle(obj, filename):
	"""
	Save an object as a .pkl file, with specified filename (no need to add .pkl suffix).
	The filename must be a string.
	Filepath is already specified as: 'data/' + filename + '.pkl'
	"""
	with open('data/' + filename + '.pkl', 'wb') as f:
		pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)
		print('Saved data/' + filename + '.pkl')


def load_pickle(name):
	"""
	Load a .pkl file from the `data/` folder.
	The name must be a string.
	Filepath is already specified as: 'data/' + name + '.pkl'
	"""
	with open('data/' + name + '.pkl', 'rb') as f:
		return pickle.load(f)



##### functions to build dict to iterate over and add info to it #####

def build_year_links_dict(name_string):
	"""
	Construct a dictionary that maps years in which there was a Tour de France 
	race with the main base URL associated with that year, given the main 
	Tour de France URL as a starting point.
	
	Takes 1 positional arguments:
		name_string: the name to be used in the saved file (must be string)

	Saves the file to the data/ folder as a .pkl file, where:
		key = year of a particular Tour de France race
		value = a dictionary containing a mapping of 'mainurl' with the 
			main URL of that year
	"""
	start_url = 'https://www.letour.fr/en/history'
	r = requests.get(start_url)
	soup = BeautifulSoup(r.text, 'html.parser')
	results = soup.find_all('button') 
	year_links_dict = {}

	for item in results: # loop through buttons for years
		if item.text:
			key = int(item.text) # year as the key
			value = {'mainurl':item['data-tabs-ajax']} # dict mapping mainurl to the main url as the first value
			year_links_dict[key] = value
			print(key, value)

	save_pickle(year_links_dict, name_string)


def add_links_to_dict(year_links_dict, name_string):
	"""
	Loop through the year-link dictionary and add links to other tables
	containing data on starters, stages, jersey wearers, stage winners 
	and rankings.
	
	Takes 2 positional arguments:
		year_links_dict
		name_string: the name to be used in the saved file (must be string)
		
	Returns: nothing.
	Saves the modified dictionary to the data/ folder as a .pkl file.
	"""
	base_url = 'https://www.letour.fr'

	for year in list(year_links_dict.keys()):
		print('Main URL: ', year, year_links_dict[year]['mainurl'])
		r = requests.get(base_url + year_links_dict[year]['mainurl'])
		soup = BeautifulSoup(r.text, 'html.parser')

		new_dict = {}
		buttons = soup.find_all('button', class_="js-tabs-nested")
		for b in buttons: # links for starters, stages, jersey wearers, stage winners and rankings
			new_key = b.text.lower().replace(' ', '_') + '_url'
			new_val = b['data-tabs-ajax']
			year_links_dict[year][new_key] = new_val
	
	save_pickle(year_links_dict, name_string)


def add_number_of_stages_to_dict(year_links_dict, name_string):
	"""
	Loop through the year-links dictionary and add to the dictionary the 
	number of stages that took place in that year's Tour de France.

	Takes 2 positional arguments:
		year_links_dict
		name_string: the name to be used in the saved file (must be string)
	Saves the modified dictionary to the data/ folder as a .pkl file.
	"""
	for year in list(year_links_dict.keys()):
		year_links_dict[year]['num_of_stages'] = scrape_number_of_stages(year_links_dict, year)
	save_pickle(year_links_dict, name_string)



##### functions to scrape data from www.letour.fr #####

def scrape_number_of_stages(year_links_dict,year):
	"""
	Scrape the number of stages (i.e., the number of racing days in the 
	Tour de France) for a given year.
	
	Takes 2 positional arguments:
		years_links_dict
		year
	Returns: the number of stages that took place in a given year.
	"""
	print(f'Scraping number of stages for {year}')
	base_url = 'https://www.letour.fr'
	r = requests.get(base_url + year_links_dict[year]['mainurl'])
	soup = BeautifulSoup(r.text, 'html.parser')
	return int(soup.find_all('span', class_='statsInfos__number')[0].text)


def scrape_starters_and_nationality(year_links_num_dict, year):
	"""
	Scrape data from the starters table for a given year.
	Takes 2 positional arguments:
		year_links_num_dict
		year: the year to scrape the data from
	Saves a CSV to the corresponding year folder in the data/ folder.
	"""
	print(f'Scraping starters and nationalities for {year}')
	base_url = 'https://www.letour.fr'
	r = requests.get(base_url + year_links_num_dict[year]['starters_url'], timeout=10)
	soup = BeautifulSoup(r.text, 'html.parser')
	rows = soup.find_all('tr')

	rows_list = []

	for i, row in enumerate(rows):

		rows_list.append(row.text.strip())

		results = row.find_all('span')
		if len(results) > 0:
			nationality = results[0].get('class')[-1]
			rows_list.append(nationality)

	starters_list = []

	for row in rows_list:

		matched_on_team = re.search(r'^[A-Z]', row)
		matched_on_nat = re.search(r'^[a-z]{3}', row)

		if matched_on_team: # if row is a team name
			new_row = [row,None,None,None]
			starters_list.append(new_row)
		elif matched_on_nat: # if row is a nationality
			new_row = [None,None,None,row]
			starters_list.append(new_row)
		else: # row is a rider
			new_row = [None]
			new_row.append([x.strip() for x in row.split('\n ')][0])
			new_row.append([x.strip() for x in row.split('\n ')][1])
			new_row.append(None)
			starters_list.append(new_row)

	starters_df = pd.DataFrame(starters_list, columns=['team','rider_num','rider_name','nationality'])
	starters_df.team = starters_df.team.fillna(method='ffill')
	starters_df.rider_num = starters_df.rider_num.fillna(method='ffill')
	starters_df.rider_name = starters_df.rider_name.fillna(method='ffill')
	starters_df = starters_df[starters_df.nationality.isnull() == False]
	starters_df = starters_df.reset_index(drop=True)
	filepath = 'data/' + str(year) + '/' + str(year) + '_starters_nationalities.csv'
	starters_df.to_csv(filepath, index=False)
	print('Saved ' + filepath)    


def scrape_stages(year_links_num_dict, year):
	"""
	Scrape data from the stages table for a given year.
	Takes 2 positional arguments:
		year_links_num_dict
		year: the year to scrape the data from
	Saves a CSV to the corresponding year folder in the data/ folder.    
	"""
	print(f'Scraping stages for {year}')
	base_url = 'https://www.letour.fr'
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
	filepath = 'data/' + str(year) + '/' + str(year) + '_stages.csv'
	stages_df.to_csv(filepath, index=False)
	print('Saved ' + filepath)    


def scrape_jersey_wearers(year_links_num_dict, year):
	"""
	Scrape data from the jersey wearers table for a given year.
	Takes 2 positional arguments:
		year_links_num_dict
		year: the year to scrape the data from
	Saves a CSV to the corresponding year folder in the data/ folder.    
	"""
	print(f'Scraping jersey wearers for {year}')
	base_url = 'https://www.letour.fr'
	r = requests.get(base_url + year_links_num_dict[year]['jersey_wearers_url'], timeout=10)
	soup = BeautifulSoup(r.text, 'html.parser')
	jersey_list = []
	num_of_cols = 5
	
	rows = soup.find_all('tr')
	for row in rows:
		new_row = row.text.strip('\n')
		new_row = re.sub(r'\s\s+',',',new_row)
		#new_row = re.sub(r',$','',new_row) # commented out to prevent column number not matching issue
		jersey_list.append(new_row)

	jersey_list = [i.split(',') for i in jersey_list]
	header = jersey_list.pop(0)
	header = [x.lower().replace(' ', '_') for x in header[0].split('\n')] # modified header code to fix col match bug
	header[0] = 'stage_num'
	jersey_df = pd.DataFrame(jersey_list)
	
	while len(header) > len(jersey_df.columns): # while header is longer than num of cols in df
		header.pop()
	
	while len(jersey_df.columns) > num_of_cols: # while df has more columns than it should (fixes column number not matching bug)
		jersey_df = jersey_df.drop(jersey_df.columns[len(jersey_df.columns)-1], axis=1)
	
	jersey_df = jersey_df.replace('', np.nan)  # added to fix bug
	jersey_df = jersey_df.dropna(axis=1, how='all')  # added to fix bug on header not matching number of columns in df
	jersey_df.columns = header
	filepath = 'data/' + str(year) + '/' + str(year) + '_jersey_wearers.csv'
	jersey_df.to_csv(filepath, index=False)
	print('Saved ' + filepath)    


def scrape_stage_winners(year_links_num_dict, year):
	"""
	Scrape data from the stage winners table for a given year.
	Takes 2 positional arguments:
		year_links_num_dict
		year: the year to scrape the data from
	Saves a CSV to the corresponding year folder in the data/ folder.    
	"""
	print(f'Scraping stage winners for {year}')
	base_url = 'https://www.letour.fr'
	r = requests.get(base_url + year_links_num_dict[year]['stages_winners_url'], timeout=10)
	soup = BeautifulSoup(r.text, 'html.parser')
	stage_winners_list = []

	rows = soup.find_all('tr')
	for row in rows:
		new_row = row.text.strip('\n')
		new_row = re.sub(r'\s\s\s+',',',new_row) # added a third \s to fix bug of splitting on team name in parens
		new_row = re.sub(r',$','',new_row)
		new_row = re.sub(r'\n',',',new_row)
		stage_winners_list.append(new_row)

	stage_winners_list = [i.split(',') for i in stage_winners_list]
	stage_winners_list.pop(0)
	header = ['stage_num','parcours','winner','team']
	stage_winners_df = pd.DataFrame(stage_winners_list
									, columns=header
								   )
	filepath = 'data/' + str(year) + '/' + str(year) + '_stage_winners.csv'
	stage_winners_df.to_csv(filepath, index=False)
	print('Saved ' + filepath)


def scrape_all_rankings(year_links_num_dict, year):
	"""
	Scrape data from the all of the rankings tables for a given year.
	Takes 2 positional arguments:
		year_links_num_dict
		year: the year to scrape the data from
	Saves a CSV to the corresponding year folder in the data/ folder.    
	"""
	print(f'Scraping rankings for all codes for {year}')
	base_url = 'https://www.letour.fr'
	ranking_cats = {'indiv_general':'itg',
				'indiv_stage':'ite',
				'points_general':'ipg',
				#'points_stage':'ipe',  # not interested in this one
				'climber_general':'img',
				#'climber_stage':'ime',  # not interested in this one
				'youth_general':'ijg',
				#'combative_general':'icg',  # not interested in this one
				'team_stage':'ete',
				'team_general':'etg'
			   }

	for label, code in ranking_cats.items(): # loop through ranking codes
		print(label, code)
		num_columns = -1 # added this code block to fix header length not matching number of cols of data bug

		if code == 'itg' or code == 'ite':
			header = ['rank','rider','rider_no','team','times','gap','b','p']
			num_columns = 8
		elif code == 'ipg':
			header = ['rank','rider','rider_no','team','points','b','p']
			num_columns = 7
		elif code == 'img':
			header = ['rank','rider','rider_no','team','points']
			num_columns = 5
		elif code == 'ijg':
			header = ['rank','rider','rider_no','team','times','gap']
			num_columns = 6
		elif code == 'ete' or code == 'etg':
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
			time.sleep(3) # too short?
			soup = BeautifulSoup(r.text, 'html.parser')
			print(full_url)

			rows_for_df = {} 
			row_num = 0
			for item in soup.tbody.find_all('tr'):
				row = item.find_all('td')

				if len(row) == num_columns: # check for if number of columns in header matches

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



##### functions to build dataframes from csv data #####

def build_stages_dataframe(year_links_num_dict):
    """
    Concatenate all the stages CSV data from all years into a dataframe.
    Takes 1 positional argument:
    	year_links_num_dict
    Saves a file, 'stages_all.pkl', in the data/ folder.
    """
    year_range = list(reversed(list(year_links_num_dict.keys())))

    stages = pd.DataFrame()

    for year in year_range:
        filepath = 'data/' + str(year) + '/' + str(year) + '_stages.csv'
        new_df = pd.read_csv(filepath)
        new_df['year'] = year
        stages = pd.concat([stages, new_df])
    stages = stages[['year','stage_num','date_start','start_city','finish_city']]
    stages['date_start'] = pd.to_datetime(stages.date_start)
    stages = stages.reset_index(drop=True)
    save_pickle(stages, 'stages_all')


def build_stage_winners_dataframe(year_links_num_dict):
    """
    Concatenate all the stage winners CSV data from all years into a dataframe.
    Takes 1 positional argument:
    	year_links_num_dict
    Saves a file, 'stage_winners_all.pkl', in the data/ folder.
    """
    year_range = list(reversed(list(year_links_num_dict.keys())))

    stage_winners = pd.DataFrame()

    for year in year_range:
        filepath = 'data/' + str(year) + '/' + str(year) + '_stage_winners.csv'
        new_df = pd.read_csv(filepath)
        new_df['year'] = year
        stage_winners = pd.concat([stage_winners, new_df])
    stage_winners = stage_winners[['year','stage_num','parcours','winner','team']]
    stage_winners = stage_winners.reset_index(drop=True)
    save_pickle(stage_winners, 'stage_winners_all')


def build_jersey_wearers_dataframe(year_links_num_dict):
    """
    Concatenate all the jersey wearers CSV data from all years into a dataframe.
    Takes 1 positional argument:
        year_links_num_dict
    Saves a file, 'jersey_wearers_all.pkl', in the data/ folder.
    """
    year_range = list(reversed(list(year_links_num_dict.keys())))

    jersey_wearers = pd.DataFrame()

    for year in year_range:
        filepath = 'data/' + str(year) + '/' + str(year) + '_jersey_wearers.csv'
        new_df = pd.read_csv(filepath)
        new_df['year'] = year
        jersey_wearers = pd.concat([jersey_wearers, new_df], sort='False')
    jersey_wearers = jersey_wearers[['year','stage_num','yellow_jersey','green_jersey','polka_dot_jersey','polka-dot_jersey','white_jersey']]
    jersey_wearers['polka_dot_jersey'] = jersey_wearers['polka_dot_jersey'].fillna(jersey_wearers['polka-dot_jersey'])
    jersey_wearers = jersey_wearers.drop('polka-dot_jersey', axis=1)
    jersey_wearers = jersey_wearers.reset_index(drop=True)
    save_pickle(jersey_wearers, 'jersey_wearers_all')


def build_starters_dataframe(year_links_num_dict):
    """
    Concatenate all the starters and nationalities CSV data from all years into a dataframe.
    Takes 1 positional argument:
        year_links_num_dict
    Saves a file, 'starters_all.pkl', in the data/ folder.    
    """
    year_range = list(reversed(list(year_links_num_dict.keys())))

    starters = pd.DataFrame()

    for year in year_range:
        filepath = 'data/' + str(year) + '/' + str(year) + '_starters_nationalities.csv'
        new_df = pd.read_csv(filepath)
        new_df['year'] = year
        starters = pd.concat([starters, new_df], sort='False')
    starters = starters[['year','team','rider_num','rider_name','nationality']]
    starters = starters.reset_index(drop=True)
    save_pickle(starters, 'starters_all')


if __name__ == '__main__':

	## 1. build year-links dict
	build_year_links_dict('example_dict')
	example_dict = load_pickle('example_dict')

	## 2. add links to dict for starters, stages, jersey wearers, stage winners and rankings
	add_links_to_dict(example_dict, 'example_dict_links')
	example_dict_links = load_pickle('example_dict_links')

	## 3. add number of stages to dict
	add_number_of_stages_to_dict(example_dict_links, 'example_dict_links_num')
	example_dict_links_num = load_pickle('example_dict_links_num')

	## 4. loop through all years and scrape data from each table
	dict_to_loop_over = load_pickle('example_dict_links_num') 
	
	for year in list(reversed(list(dict_to_loop_over.keys()))):
		directory = 'data/' + str(year)
		if not os.path.exists(directory):
			os.makedirs(directory)

		scrape_all_rankings(dict_to_loop_over, year)  # a) scrape rankings data
		time.sleep(3)
		scrape_starters_and_nationality(dict_to_loop_over, year)  # b) scrape starters and nationality data
		time.sleep(3)
		scrape_stages(dict_to_loop_over, year)  # c) scrape stages data
		time.sleep(3)
		scrape_jersey_wearers(dict_to_loop_over, year)  # d) scrape jersey wearers data
		time.sleep(3)
		scrape_stage_winners(dict_to_loop_over, year)  # e) scrape stage winners data
		time.sleep(3)	

	## 5. build dataframes for each of the data tables
	build_stages_dataframe(dict_to_loop_over)
	build_stage_winners_dataframe(dict_to_loop_over)
	build_jersey_wearers_dataframe(dict_to_loop_over)
	build_starters_dataframe(dict_to_loop_over)