import scrapy,re
from bs4 import BeautifulSoup

class DWSpider(scrapy.Spider):
    name = 'dw'
    start_urls = ['http://www.chakoteya.net/DoctorWho']

    doctors = {
        'Jodie Whittaker':13,
        'Twelfth Doctor':12,
        'Seventh Doctor':7,
        'First Doctor':1,
        'Second Doctor':2,
        'Eleventh Doctor':11,
        'Third Doctor':3,
        'Fourth Doctor':4,
        'Eighth Doctor':8,
        'Tenth Doctor':10,
        'Sixth Doctor':6,
        'Fifth Doctor':4,
        'Ninth Doctor':9
    }

    '''
    This function gets the link and splits it
    Examples:

    1-0.html    -> 1, 0. 0
    1-1-1.html  -> 1, 1, 1
    A.html      -> 0, 0, 0

    The numeric episodes are required in order to sort the dataset by the appearence of episodes.
    It is critical for dataset built with scrapy because scrapy crawls the link asynchroniously

    '''

    def __get_episode__(self,episode_link):
        splitted = re.split('[-|.]', episode_link)
        if len(splitted) == 2:
            return (0, 0, 0)
        elif len(splitted) == 4:
            return (int(splitted[0]), int(splitted[1]), int(splitted[2]))
        else:
            return (int(splitted[0]), int(splitted[1]), 0)

    '''
    This function gets the text separated by new line and sequuentally builds a dataset from it in he following way:
    1. Marks and enumerates parts and scenese by id and name
    2. Tags If the sentence is a talk, context or location
    3. Splits the talk sentences into "talker" and "talk" iself

    Part_id and part_name are required for marking teh scripts devided into parts or episodes
    Example

    (narrative)
    [Tardis]
    DOCTOR: Hello, Dalek
    DALEK: EXTERMINATE

    'part_id', 'part_name', 'scene_id','scene_name', 'text',        'phrase_type', 'detail'
     0          ''           0          0             narrative      context       NaN
     0          ''           1          Tardis        NaN            location      NaN
     0          ''           1          Tardis        Hello, Dalek   talk          DOCTOR
     0          ''           1          Tardis        EXTERMINATE    talk          DALEK 

    '''

    def __parse_lines__(self,lines):
        details = ''
        phrase_type = 'talk'
        scene_id = 0
        scene_name = ''
        text = ''
        part_id = 0
        part_name = ''
        results = []
        for line in lines:
            # scene (location) is found
            if line.startswith('['):
                scene_id += 1
                scene_name = line.replace('\n', ' ').strip()
                details = ''
                text = ''
                phrase_type = 'location'
            # context is found
            elif line.startswith('('):
                details = ''
                text = line.replace('\n', ' ').strip()
                phrase_type = 'context'
            # talk is found
            elif len(re.findall("[A-Z]*: ", line)) > 0:
                sent = re.split(r"([A-Z]*: )", line)
                details = sent[1].split(':')[0].strip()
                text = sent[2].strip()
                phrase_type = 'talk'
            # back link is found (to handle some xpth problem, relevant for scrapy)
            elif line.startswith('<Back'):
                break
            # episode or part is found
            elif line.startswith('Episode') or line.startswith('Part'):
                # print(line)
                part_id += 1
                part_name = line.replace('\n', ' ').strip()
                details = ''
                text = ''
                phrase_type = 'episode'
            # handle current line as it belongs to previously found type
            else:
                text = line.replace('\n', ' ').strip()
            results.append([part_id, part_name, scene_id, scene_name, text, phrase_type, details])
        return results


    def parse(self,response):
        for element in response.xpath('//a[contains(@href,"episode")][img]')[12:13]:
            link = element.xpath(".//@href").get()
            doctor = element.xpath(".//img/@alt").get()
            yield response.follow(link, callback=self.parse_episodes, meta={'doctor':doctor})



    def parse_episodes(self,response):
        doctor = response.request.meta['doctor']
        counter = 0
        for row in response.xpath('//td/table[@border="1"][1]/tbody/tr[td/@bgcolor!="#006b9f"]'):
            counter+=1
            episode_name = row.xpath(".//td[1]//a//text()").get()
            episode_link = row.xpath(".//td[1]//a//@href").get()
            if episode_link is None:
                break
            ord_season_id, episode_id_1, episode_id_2 = self.__get_episode__(episode_link)

            meta_data = {
                'doctor':doctor,
                'doctor_id': self.doctors[doctor],
                'episode_name': episode_name,
                'ord_season_id': ord_season_id,
                'episodeid' : episode_link.split('.')[0],
                'episode_id_1': episode_id_1,
                'episode_id_2': episode_id_2
            }
            yield {'link':episode_link,'name' :episode_name, 'counter': counter}



    def parse_single_episode(self, response):
        soup = BeautifulSoup(response.xpath('//td')[0].get(), "html5lib")
        # kill all script and style elements
        for script in soup(['script','style']):
            script.decompose()  # rip it out
        processed_text = self.__parse_lines__(soup.stripped_strings)
        doctor = response.request.meta['doctor']
        doctor_id = response.request.meta['doctor_id']
        episode_name = response.request.meta['episode_name']
        episodeid = response.request.meta['episodeid']
        ord_season_id = response.request.meta['ord_season_id']
        episode_id1 = response.request.meta['episode_id_1']
        episode_id2 = response.request.meta['episode_id_2']
        for record in processed_text:
            yield {'doctor':doctor,
                'doctor_id':doctor_id,
                'episode_name': episode_name,
                'episodeid':episodeid,
                'ord_season_id': ord_season_id,
                'episode_id_1': episode_id1,
                'episode_id_2': episode_id2,
                'part_id':record[0],
                'part_name': record[1],
                'scene_id': record[2],
                'scene_name' : record[3],
                'text': record[4],
                'phrase_type': record[5],
                'detail': record[6]
               }
