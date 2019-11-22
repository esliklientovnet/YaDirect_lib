import json
from io import StringIO
from time import sleep

import pandas as pd
import requests
from requests.exceptions import ConnectionError

from settings import *




class yaDirect():
    """Class for Yandex Direct API"""

    def __init__(self, token, login, apiVersion=5):
        self.login = login
        self._token = settings.token
        self.apiVersion = apiVersion
        # self.yaDirectUrl = url = "https://api-sandbox.direct.yandex.com/json/v%s/" % self.apiVersion
        self.yaDirectUrl = url = "https://api.direct.yandex.com/json/v%s/" % self.apiVersion

    def set_Login(self, login):
        self.login = login

    def requestAPI(self, service, method, params, login=''):
        """
        Запрос в Яндекс Директ
        возвращает объект requests.Response
        """
        req = ''
        if login == '':
            login = self.login
        headers = {
            'Authorization': 'Bearer ' + self._token,
            'Accept-Language': 'en',
            'Client-Login': login,
            'processingMode': 'auto'
        }
        # Формируем URL сервиса
        url = self.yaDirectUrl + service
        # Для сервиса отчетов не используем method
        if method == "":
            data = {'params': params}
        else:
            data = {
                'method': method,
                'params': params
            }
        # Результат в случае ошибки
        result = "error"
        # конвертируем словарь в JSON-формат и перекодируем в UTF-8
        jdata = json.dumps(data, ensure_ascii=False).encode('utf8')

        while True:
            try:
                req = requests.post(url, jdata, headers=headers)
                # Отладочная информация
                # print("Стутус: {}".format(req.status_code))
                # print("Заголовки запроса: {}".format(req.request.headers))
                # print("Запрос: {}".format(u(req.request.body)))
                # print("Заголовки ответа: {}".format(req.headers))
                # print("Ответ: {}".format(u(req.text)))
                # print("\n")
                # Обработка запроса
                if 'LimitedBy' in req.text:
                    if req.json()['result'].get('LimitedBy', False):
                        # Если ответ содержит параметр LimitedBy, значит,  были получены не все доступные объекты.
                        # В этом случае следует выполнить дополнительные запросы для получения всех объектов.
                        # Подробное описание постраничной выборки - https://tech.yandex.ru/direct/doc/dg/best-practice/get-docpage/#page
                        print("Получены не все доступные объекты.")

                if req.status_code == 200:
                    #print("Запрос выполнен успешно")
                    #print("RequestId: {}".format(req.headers.get("RequestId", False)))
                    break
                elif req.status_code == 201:
                    print("Отчет успешно поставлен в очередь в режиме офлайн")
                    retryIn = int(req.headers.get("retryIn", 60))
                    print("Повторная отправка запроса через {} секунд".format(retryIn))
                    print("RequestId: {}".format(req.headers.get("RequestId", False)))
                    sleep(retryIn)
                elif req.status_code == 202:
                    print("Отчет формируется в режиме офлайн")
                    retryIn = int(req.headers.get("retryIn", 60))
                    print("Повторная отправка запроса через {} секунд".format(retryIn))
                    print("RequestId:  {}".format(req.headers.get("RequestId", False)))
                    sleep(retryIn)
                elif req.status_code == 400:
                    print("Параметры запроса указаны неверно или достигнут лимит отчетов в очереди")
                    print("RequestId: {}".format(req.headers.get("RequestId", False)))
                    print("JSON-код запроса: {}".format(u(jdata)))
                    print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                    break
                elif req.status_code == 500:
                    print("При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее")
                    print("RequestId: {}".format(req.headers.get("RequestId", False)))
                    print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                    break
                elif req.status_code == 502:
                    print("Время формирования отчета превысило серверное ограничение.")
                    print(
                        "Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.")
                    print("JSON-код запроса: {}".format(jdata))
                    print("RequestId: {}".format(req.headers.get("RequestId", False)))
                    print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                    break
                else:
                    print("Код ответа сервера", req.status_code)
                    print("Произошла ошибка при обращении к серверу API Директа.")
                    print("Код ошибки: {}".format(req.json()["error"]["error_code"]))
                    print("Описание ошибки: {}".format(u(req.json()["error"]["error_detail"])))
                    print("RequestId: {}".format(req.headers.get("RequestId", False)))
                    print("Информация о баллах: {}".format(req.headers.get("Units", False)))
                    break




            # Обработка ошибки, если не удалось соединиться с сервером API Директа
            except ConnectionError:
                # В данном случае мы рекомендуем повторить запрос позднее
                print("Произошла ошибка соединения с сервером API")
                # Принудительный выход из цикла
                break

            # Если возникла какая-либо другая ошибка
            except Exception as e:
                print('Произошла непредвиденная ошибка')
                print(e)
                print(e.args)
                # Принудительный выход из цикла
                break

        return req

    def get_AgencyClients(self):
        """"Возвращает список клиентов агентства"""
        service = 'agencyclients'

        # Подготавливаем данные
        method = "get"
        params = {
            "SelectionCriteria": {},
            "FieldNames": ["ClientId", "ClientInfo", "Login"]
        }
        result = self.requestAPI(service, method, params)
        clients = result.json()
        return clients["result"]["Clients"]

    def get_Regions(self):
        """"Возвращает список регионов"""
        # Подготавливаем данные
        service = 'dictionaries'
        method = "get"
        params = {
            "DictionaryNames": ["GeoRegions"]
        }

        result = self.requestAPI(service, method, params)
        regions = result.json()
        return regions["result"]["GeoRegions"]

    def get_Campaigns(self, params={}, login=''):
        """"Возвращает список кампаний по логину"""
        service = 'campaigns'
      
        # Подготавливаем данные
        method = "get"
        result = self.requestAPI(service, method, params, login)
        return result.json()

    def update_Campaigns(self, params={}, login=''):   
        """"Возвращает список кампаний по логину"""
        service = 'campaigns'
      
        # Подготавливаем данные
        method = "update"
        result = self.requestAPI(service, method, params, login)
        return result.json()


    def get_KeywordBids(self, SelectionCriteria={}, FieldNames=["KeywordId", "AdGroupIds", "CampaignIds",  "Bid", "AuctionBids"],  login=''):
        """"Возвращает ставки у ключей по заданым критериям"""
        service = 'bids'
        # Подготавливаем данные
        method = "get"
        params = {"SelectionCriteria": SelectionCriteria,
                  "FieldNames": FieldNames
                 }
        result = self.requestAPI(service, method, params, login)
        bids=result.json()

        #return bids["result"]["Bids"]

        return bids

    def get_BidModifiers(self, SelectionCriteria={}, FieldNames=[], RegionalAdjustmentFieldNames=[], login=''):
        """"Возвращает модификаторы ставок по заданым критериям"""
        service = 'bidmodifiers'
        # Подготавливаем данные
        method = "get"
        params = {"SelectionCriteria": SelectionCriteria,
                  "FieldNames": FieldNames,
                  "RegionalAdjustmentFieldNames": RegionalAdjustmentFieldNames
                  }
        result = self.requestAPI(service, method, params, login)
        bids = result.json()

        # return bids["result"]["Bids"]

        return bids
    
    def add_BidModifiers(self, params={}, login=''):
        """"Добавляет модификаторы ставок по заданым критериям"""
        service = 'bidmodifiers'
        # Подготавливаем данные
        method = "add"
        result = self.requestAPI(service, method, params, login)
        bids = result.json()
        # return bids["result"]["Bids"]
        return bids

    def delete_BidModifiers(self, ids=[], login=''):
        """"Добавляет модификаторы ставок по заданым критериям"""
        service = 'bidmodifiers'
        # Подготавливаем данные
        method = "delete"
        params = {"SelectionCriteria":
                      {"Ids": ids}
                  }
        result = self.requestAPI(service, method, params, login)
        bids = result.json()
        # return bids["result"]["Bids"]
        return bids

    def get_AdGroups (self, params={}, login=''):
        """"Получает группы по заданым критериям"""
        service = 'adgroups'
        # Подготавливаем данные
        method = "get"
        result = self.requestAPI(service, method, params, login)
        return result.json()

    def update_AdGroups (self, params={}, login=''):
        """"Обновляет группы по заданым критериям"""
        service = 'adgroups'
        # Подготавливаем данные
        method = "update"
        result = self.requestAPI(service, method, params, login)
        return result.json()




    def Reports(self, params):
        service = 'reports'
        method = ""
        result = self.requestAPI(service, method, params)
        return result.text

    def tsvToPandasDF(self, input):
        IOResult = StringIO(input)
        df = pd.read_csv(IOResult, sep="\t", skiprows=1, skipfooter=1)
        return df
