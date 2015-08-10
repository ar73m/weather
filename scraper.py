# -*- coding: utf-8 -*-
import scraperwiki
import lxml.html
import os
import time
import urlparse

#from concurrent.futures import ThreadPoolExecutor
#import threading
from datetime import datetime
from grab import Grab

USER_AGENT = "Mozilla"#os.environ['MORPH_USER_AGENT']
THREADS = 10

def scrape_cities(g):
    resp = g.go(url='http://meteo.gov.ua/')
    html_str = resp.body
    page = lxml.html.fromstring(html_str)
    
    cities = []
    
    div_city = page.xpath('//div[@class="selec1"]')[0]
    links_city = div_city.xpath('.//a[@class="m13"]')
    for link in links_city:
        city = {}
        city['url'] = link.xpath('./@href')[0]
        city['title'] = link.xpath('./text()')[0]
        city['id'] = city['url'].split('/')[-1]
        
        cities.append(city)
        
    return cities

def add_to_database(result):
    pass

def parse_city(g, city):
    try:
        url = city['url']
        resp = g.go(url=url)
        html_str = resp.body
        page = lxml.html.fromstring(html_str)
        
        weather = {}
        
        title_city = page.xpath('//div[@class="hdr_fr_bl1_sity"]/text()')[0].strip()
        
        sun_div = page.xpath('//div[@class="sun"]')[0]
        sunrise, suntime, sunset = "", "", ""
        try:
            suntime = sun_div.xpath('div[2]/text()')[0].strip()
            sunrise = sun_div.xpath('div[1]/text()')[0].strip()
            sunset = sun_div.xpath('div[3]/text()')[0].strip()
        except Exception:
            pass
        
        date = page.xpath('//div[@class="hdr_fr_bl1_date"]/text()')[0].strip()
        
        current_weather_div = page.xpath('//div[@class="hdr_fr_bl2"]')[0]
        
        time = page.xpath('//span[@id="curWeatherHour"]/text()')[0].strip()
            
        temp = current_weather_div.xpath('.//span[@id="curWeatherT"]/text()')[0].strip()
        wind_speed = current_weather_div.xpath('.//span[@id="curWeatherWS"]/text()')[0].strip()
        humidity = current_weather_div.xpath('.//span[@id="curWeatherHu"]/text()')[0].strip()
        pres = current_weather_div.xpath('.//span[@id="curWeatherPr"]/text()')[0].strip()

    
        weather['url'] = city['url']
        weather['city'] = {}
        weather['city']['title'] = title_city
        weather['city']['id'] = city['id']
        weather['sunrise'] = sunrise
        weather['suntime'] = suntime
        weather['sunset'] = sunset
        weather['weather'] = {}
        weather['weather']['datetime'] = u'{0} {1}:00'.format(date, time)
        weather['weather']['temp'] = u'{0} °C'.format(temp)
        weather['weather']['wind'] = u'{0} м/с'.format(wind_speed)
        weather['weather']['hu'] = u'{0} %'.format(humidity)
        weather['weather']['pres'] = u'{0} мм. рт. ст.'.format(pres)
        
        return weather
    except Exception, e:
        print("Error: {0}, URL: {1}".format(e, url))
    
def scrape_weather(urls):
        grabs = [Grab(user_agent=USER_AGENT) for _ in range(THREADS)]
    #with ThreadPoolExecutor(max_workers=THREADS) as executor:
        for i, url in enumerate(urls):
            future = executor.submit(parse_city, grabs[i % THREADS], url)
            future.add_done_callback(add_to_database)
        

def start_scrape():
    # Init Grab
    print('Start {}'.format(datetime.strftime(datetime.now(), u'%Y/%m/%d %H:%M:%S')))
    g = Grab(timeout=10)
    g.setup(user_agent=USER_AGENT)
    cities = scrape_cities(g)
    #print(cities)
    
    for city in cities:
        try:
            w = parse_city(g, city)
            print("Parse {}".format(city['title'].encode('cp866', 'replace')))
            scraperwiki.sqlite.save(unique_keys=['id'], 
                    data={  'id': w['city']['id'],
                            'city': w['city']['title'],
                            'url': w['url'], 

                            'sun': u"{0} - {1} - {2}".format(w['sunrise'], w['suntime'], w['sunset']),
                            'temp': w['weather']['temp'],
                            'wind': w['weather']['wind'],
                            'hu': w['weather']['hu'],
                            'pres': w['weather']['pres'],
                            'datetime': w['weather']['datetime'],

                            'update_date': datetime.strftime(datetime.now(), u'%Y/%m/%d %H:%M:%S')
                            })
        except Exception, e:
            print("Error: {0}, URL: {1}".format(e, city['url']))
            
    print('End {}'.format(datetime.strftime(datetime.now(), u'%Y/%m/%d %H:%M:%S')))

if __name__ == '__main__':
    start_scrape()