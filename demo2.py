from pymongo.errors import DuplicateKeyError
from selenium import webdriver
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait
import logging
from urllib.parse import urljoin
import pymongo
import time
import json
import random
import asyncio
import aiohttp
from selenium.common.exceptions import NoSuchElementException
import random
import hashlib
import re



#这是pymongo的配置
client = pymongo.MongoClient(host='localhost')
db = client['jd']
collection = db["selejd"]
collection.create_index([('pn_id', 1)], unique=True)
#配置logging，同时也是设定logging的格式
logging.basicConfig(level= logging.INFO,format='%(asctime)s - %(levelname)s: %(message)s')

url_demo = 'https://search.jd.com/Search?keyword=%E6%89%8B%E6%9C%BA&pvid=03cf2296e3d2408b883bc0d4e4d7810a&isList=0&page={}'

#构造列表页的url
INDEX_URL = ''
#设置最长等待时间
TIME_OUT = 10
#最大的翻页页数是10
TOTAL_PAGE = 100
#设置一个浏览器对象
options = webdriver.ChromeOptions()
options.add_argument('user-agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"')

browser = webdriver.Chrome(options=options)

browser.execute_cdp_cmd('Page.addScriptToEvaluateOnNewDocument',
                       {'source': 'Object.defineProperty(navigator, "webdriver", {get: () => undefined})'})

#可以配置页面加载的最长等待时间
wait = WebDriverWait(browser,TIME_OUT)
#通用爬虫方法,用来爬取页面信息，通过定位标签来爬取信息

#初始页面:https://www.jd.com/
login_URL = 'https://passport.jd.com/uc/login?ltype=logout'
ORGIN_URL = 'https://www.jd.com/'
goods_name = '耳机'
#获取登录的cookies，生成一个json文件来存储cookies



def click_retry_until_gone(browser):
    while True:
        logging.info('定位到了重试字体')
        retry_buttons = browser.find_elements(By.XPATH,'//*[@id="J_scroll_loading"]/span/a/font')
        if not retry_buttons:  # 如果列表为空，说明没有"重试"按钮
            break
        retry_buttons[0].click()  # 点击找到的第一个"重试"按钮
        time.sleep(2)


def SEND_TAOBAO_cookies(url):
    browser.get(url)
    #WebDriverWait(browser, 20).until(EC.url_changes(browser.current_url))  # 等待URL发生变化
    #print(browser.current_url)
    login_code = browser.find_element(By.XPATH, '//div[@class="login-form-body"]/div[@class="login-tab login-tab-l"]/a')
    login_code.click()
    WebDriverWait(browser, 60).until(EC.url_changes(browser.current_url))
    print(browser.get_cookies())
    with open('cookies.json', 'w') as f:
        f.write(json.dumps(browser.get_cookies()))
#用于模拟输入搜索词语和点击搜索按钮
def INDEX_PAGE(goods_name):
    ssuo = browser.find_element(By.XPATH,'//div[@id="search"]/div[@class="search-m"]/div[@class="form"]/input[@id="key"]')
    ssuo.send_keys(goods_name)
    ssuo_button = browser.find_element(By.XPATH,'//button[@class="button"]')
    ssuo_button.click()
    print(browser.current_url)
    WebDriverWait(browser, 20).until(EC.url_changes(browser.current_url))
    #browser.quit()

#用来解析列表页的信息。同时爬取详情页的href，返回一个详情页的url
def parse_index():
    results = []
    old_len = 0
    while len(results) < 60:  # 只有当results的长度小于60时，才继续循环
        logging.info('爬取页面的数量：%s',len(results))
        click_retry_until_gone(browser)
        # 分段滚动到页面底部
        for i in range(10):
            browser.execute_script("window.scrollBy(0, window.innerHeight);")
            time.sleep(1)  # 每次滚动后等待2秒让页面加载

        # 获取所有的元素
        elements = browser.find_elements(By.XPATH, '//ul[@class="gl-warp clearfix"]/li')

        # 如果新加载的元素数量和旧的数量相同，说明已经滑到底部
        if len(elements) == old_len:
            break

        # 解析新加载的元素
        for element in elements[old_len:]:
            href = element.find_element(By.XPATH, './/div[@class="p-img"]/a').get_attribute('href')
            result = {
                'url': href,
            }
            results.append(result)
            if len(results) >= 60:  # 如果results的长度达到或超过60，就跳出循环
                break

        # 更新元素数量
        old_len = len(elements)

    return results

#用于发起请求，同时添加cookies，达到免扫码登录的效果
def scrape_page(url,condition,locator):
    logging.info('scraping %s',url)
    try:
        # browser对象发起请求
        browser.get(url)
        #打开生成的json文件，将cookies添加网址中
        with open('cookies.json', 'r') as f:
            cookies = json.load(f)
            for cookie in cookies:
                browser.add_cookie(cookie)
        #进行一次刷新，重新获取登录的状态
        browser.refresh()
        #等待特定条件(condition)的元素或页面元素(locator)出现
        wait.until(condition(locator))
    except TimeoutException:
        logging.error('error occurred while scraping %s,url,exc_info =True')


#这个是不用添加cookies的请求方法
def send_request(url,condition,locator):
    logging.info('请求的url:%s',url)
    try:
        browser.get(url)
        wait.until(condition(locator))
    except TimeoutException:
        logging.error('error occurred while scraping %s,url,exc_info =True')

#md5加密
def md5_encode(str):
    m = hashlib.md5()
    m.update(str.encode('utf-8'))
    return m.hexdigest()



#爬取详情页
def spider_detail(message):
    #爬取标题
    title = browser.find_element(By.XPATH,'//div[@class="sku-name"]').text
    #价钱
    while True:
        try:
            money_element = browser.find_element(By.XPATH,'//div/div[1]/div[2]/span[1]/span[2]')
            if money_element.text.strip():  # 如果元素的文本不为空，就跳出循环
                money = money_element.text
                break
            else:  # 否则，就点击按钮并等待
                elements = browser.find_elements(By.XPATH, '//*[@id="choose-attr-1"]/div[2]/div')
                if elements:
                    random.choice(elements).click()
                    time.sleep(3)  # 等待3秒让页面加载
                else:
                    browser.refresh()
                    time.sleep(3)
        except NoSuchElementException:
            # 如果找不到元素，就随机点击一个元素
            elements = browser.find_elements(By.XPATH, '//*[@id="choose-attr-1"]/div[2]/div')
            if elements:
                random.choice(elements).click()
                time.sleep(3)  # 等待3秒让页面加载
            else:
                browser.refresh()
                time.sleep(3)
    #商品信息的节点
    jiedian = browser.find_elements(By.XPATH,'//*[@id="detail"]/div[2]/div[1]/div[1]/ul[2]/li')
    logging.info('节点信息：%s',len(jiedian))
    for element in jiedian:
        a = element.text
        b = a.split('：')
        logging.info('成功分割商品信息：%s',b)
        message[b[0]] = element.get_attribute('title')
    #添加标题
    message['title'] = title
    #添加价钱
    message['money'] = money
    #点击评价详情
    content_button = browser.find_element(By.XPATH,'//*[@id="detail"]/div[1]/ul/li[5]')
    content_button.click()
    #换种等待方法
    time.sleep(3)
    #获取评价节点
    content_list = browser.find_elements(By.XPATH,'//*[@id="comment"]/div[2]/div[2]/div[1]/ul/li')
    for content in content_list:
        a = content.text
        b = a.split('(')
        if len(b) == 2:
            logging.info('分割后的列表：%s',b)
            message[b[0]] = b[1].replace(')',"")
            logging.info('成功')
        else:
            continue
    #商家名称
    sjia_name = browser.find_element(By.XPATH,'//*[@id="crumb-wrap"]/div/div[2]/div[2]/div[1]/div/a').text
    message['商家名称'] = sjia_name
    #插入pn_id
    re_demo = r'\d+'
    web_id = re.findall(re_demo, browser.current_url)
    result = ''.join(map(str, web_id)) + sjia_name
    message['pn_id'] = md5_encode(result)
    #商家评分
    #模拟鼠标悬停
    score_list1 = browser.find_element(By.XPATH, '//*[@id="crumb-wrap"]/div/div[2]/div[2]/div[1]/div')
    ActionChains(browser).move_to_element(score_list1).perform()
    time.sleep(3)
    #爬取结果
    score_list = browser.find_elements(By.XPATH, '//*[@id="crumb-wrap"]/div/div[2]/div[2]/div[7]/div/div/div[1]/a')
    print(score_list)
    if score_list == None:
        logging.error('定位商家评分信息详情失败，可能是自营店不存在评分')
    else:
        for list in score_list:
            score = list.find_element(By.XPATH,'.//em').text
            logging.info('评分信息：%s',score)
            message[list.find_element(By.XPATH,'./div[1]/text()')] = score
    browser.close
    return message


if __name__ == '__main__':
    #SEND_TAOBAO_cookies(login_URL)
    num = 1
    for i in range(100):
        if num==1:
            #url = url_demo.format(num)
            scrape_page(ORGIN_URL,condition=EC.visibility_of_all_elements_located,locator=(By.TAG_NAME,'h2'))
            INDEX_PAGE(goods_name)
            detail_urls = parse_index()
            print(len(detail_urls))
            for detail_url in detail_urls:
                send_request(detail_url['url'],condition=EC.visibility_of_all_elements_located,locator=(By.XPATH,'//div[@class="sku-name"]'))
                dict = spider_detail(detail_url)
                try:
                    # 尝试插入文档
                    collection.insert_one(dict)
                except DuplicateKeyError:
                    # 如果出现重复键错误，打印pn_id并跳过
                    print("已经爬取过的网页，pn_id为：", dict['pn_id'])
                    continue
            num = num + 2
        else:
            url = url_demo.format(num)
            scrape_page(url, condition=EC.visibility_of_all_elements_located, locator=(By.TAG_NAME, 'h2'))
            #INDEX_PAGE(goods_name)
            detail_urls = parse_index()
            print(len(detail_urls))
            for detail_url in detail_urls:
                logging.info('details urls %s', detail_url)
                send_request(detail_url['url'],condition=EC.visibility_of_all_elements_located,locator=(By.XPATH,'//div[@class="sku-name"]'))
                dict = spider_detail(detail_url)
                try:
                    # 尝试插入文档
                    collection.insert_one(dict)
                except DuplicateKeyError:
                    # 如果出现重复键错误，打印pn_id并跳过
                    print("已经爬取过的网页，pn_id为：", dict['pn_id'])
                    continue
            num = num + 2
    browser.quit()

