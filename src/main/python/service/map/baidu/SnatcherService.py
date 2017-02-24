# -*- coding:utf-8 -*-

from service.map.baidu.APIService import BaiduMapAPIService
from dao.map.BaiduMapDAO import BaiduMapDAO

import multiprocessing

import logging

from util.geo.GeoUtil import GeoUtil

logger = logging.getLogger("ugc")


def frange(x, y, step):
    while x < y:
        yield x
        x += step


class BaiduMapSnatcherService(object):
    def __init__(self, host, db, user, password, ak="DW2CwL3B3271CiVyw7GdBsfR"):
        logging.debug("Constructor  ak:%s" % ak)
        self.baiduAPIService = BaiduMapAPIService(ak)
        self.baiduMapDAO = BaiduMapDAO(host, db, user, password)

    # def __del__(self):
    #     print "... Destructor BaiduMapSnatcherService...  %s" % multiprocessing.current_process().name

    # 抓取Place详情
    def fetchPlaceDetail(self, lng0, lat0, lng1, lat1, query):
        # 使用矩形范围初始栈
        queue = [[lng0, lat0, lng1, lat1]]
        # 用以存放返回值为0的矩形范围
        zero_queue = []
        while len(queue) != 0:
            # 取出一个查询范围
            range = queue.pop()
            # 根据范围进行查询
            try:
                data = self.baiduAPIService.placeSearch(query=query,
                                                    bounds="%lf,%lf,%lf,%lf" % (range[1], range[0], range[3], range[2]))
            except Exception, e:
                print e
                print u"查询数据出错"
                queue.append(range)
                continue
            # 查看数据是否有效
            try:
                print u"范围查询就结果个数： " + str(len(data['results']))
            except:
                print u"查询数据返回内容出错"
                continue
            if data.has_key('results'):
                # 如果范围的poi等于20,就切割该范围,并将切割后的子范围置入栈中
                if len(data['results']) == 20:
                    splitX = (range[0] + range[2]) / 2
                    splitY = (range[1] + range[3]) / 2
                    if (range[2] - range[0]) < 0.001 or (range[3] - range[1]) < 0.001:
                        continue
                    queue.append([range[0], splitY, splitX, range[3]])
                    queue.append([splitX, splitY, range[2], range[3]])
                    queue.append([range[0], range[1], splitX, splitY])
                    queue.append([splitX, range[1], range[2], splitY])
                    continue
                elif len(data['results']) == 0:
                    zero_queue.append(range)
                # 如果查询结果小于20则存储
                else:
                    self.baiduMapDAO.savePlaceDetail(data)
        return zero_queue

    '''
    根据extend范围调用Place API抓取POI
    '''

    def fetchPlacePagination(self, lng0, lat0, lng1, lat1, keyWords, tableName="Place_nanjing"):
        # 使用矩形范围初始栈
        queue = [[lng0, lat0, lng1, lat1]]
        bounds = queue.pop()
        data = self.baiduAPIService.placeSearchBatch(keyWords,
                                                     "%lf,%lf,%lf,%lf" % (bounds[1], bounds[0], bounds[3], bounds[2]))
        self.baiduMapDAO.savePlace(tableName, data)

        total = data["total"]
        totalPages = self.getDataTotalPage(total)
        logger.info("total:%s , totalPages: %s" % (total, totalPages))

        for pageNumber in range(1, totalPages, 1):
            data = self.baiduAPIService.placeSearchBatch(keyWords, bounds, pageNumber)
            self.baiduMapDAO.savePlace(tableName, data)

    def getDataTotalPage(self, total, pageSize=20):
        totalPages = 0
        if (total == 0):
            totalPages = 0
        else:
            if (total % pageSize > 0):
                totalPages = (int)(total / pageSize + 1)
            else:
                totalPages = (int)(total / pageSize)
        return totalPages

    def fetchPlace(self, polygon, lng0, lat0, lng1, lat1, keyWords, step=1, stepParam=0.000011836, tableName="Place"):
        '''
        根据extend范围调用Place API抓取POI
        '''

        # 使用矩形范围初始栈
        boundsQueue = [[lng0, lat0, lng1, lat1]]
        step = round(step * stepParam, 6)
        while len(boundsQueue) != 0:
            # 取出一个查询范围
            bounds = boundsQueue.pop()  # 根据范围进行查询（百度格式：纬度，经度），默认每次最多返回20条
            baiduBounds = "%lf,%lf,%lf,%lf" % (bounds[1], bounds[0], bounds[3], bounds[2])
            data = self.baiduAPIService.placeSearchBatch(keyWords, baiduBounds)
            logger.info("bounds:%s , size: %s" % (bounds, len(data['results'])))
            if data.has_key('results'):
                # 四分法遍历： 如果范围的poi等于20,就切割该范围,并将切割后的子范围置入队列
                if len(data['results']) == 20:
                    if (bounds[2] - bounds[0]) < step or (bounds[3] - bounds[1]) < step:
                        continue

                    logger.info("split bounds, boundsQueue %s" % len(boundsQueue))
                    splitedBounds = GeoUtil().splitBounds(bounds, polygon)
                    boundsQueue.extend(splitedBounds)
                    continue
                else:
                    # 小于20 存储
                    logger.info("save place %s " % len(data['results']))
                    self.baiduMapDAO.savePlace(tableName, data)

    def fetchAddressNode(self, index, points, tableName="addressnode_suzhou", start=0, placeTableName=None):
        '''
        根据点集调用反向地址编码抓取AddressNode
        '''
        total = len(points)
        logger.info('total points:%s,start:%s ' % (str(total), str(start)))
        pageSize = 1000

        # 并发数控制：每发送3000条，暂停2秒
        # print "Start : %s" % time.ctime()
        #     if (i>0) and i%3000==0:
        #          print 'sleep 2s for index %s '% i
        #          time.sleep(2)
        for start in range(start, total, pageSize):
            logger.info("thread%s ,begin:%s, total:%s" % (str(index), str(start), str(total)))
            locationList = points[start:start + pageSize]
            respList = self.baiduAPIService.reverseGeocodingBatch(locationList=locationList)
            # addressDataList = []
            # for resp in respList:
            #     if resp['status'] == 0:
            #         data = resp['result']
            #         addressDataList.append(data)
            # self.baiduMapDAO.saveAddressNode(tableName, addressDataList)
            # if placeTableName != None:
            #     self.baiduMapDAO.saveAddressNodePois(placeTableName, addressDataList)
    # 清空AddressNode表
    def truncateAddressNode(self):
        self.baiduMapDAO.truncateAddressNode("AddressNode")

    def setNullStrToNull(self,tableName):
        self.baiduMapDAO.setNullStrToNull(tableName)