#!/usr/bin/python
# migrate.py
# Author: Anirudh Parui (anirudh@ambab.com)
# Date: Dec 11, 2014
# Version: 1.0.1
# Purpose: Migrate Products from MySQL to Mongo
# Dependecies : python-pip , pymongo , mysqldb
# apt-get install python-pip ; pip install pymongo;
# apt-get install python-MySQLdb


import MySQLdb as mydb  # Import MySQL Libraimport sys, tracebackry
import pymongo as mondb  # Import MongoDB Library
import multiprocessing as mp  # Import Multiprocessing
import itertools as itt  # Import IterTools
import operator


# MySQL Config
myConfig = {'host': '<host>',
            'username': '<user>',
            'password': '<passwd>',
            'db': '<db>'}


moncon = mondb.MongoClient('mongodb://<user>:<passwd>@<host>:27017/<db>')
mondb = moncon.db.collection

# Number of threads is 3 times the cpu on the machine i.e. 3 threads per cpu
threads = 3*mp.cpu_count()

l = range(1, 20)
perm = list(itt.permutations(l, 3))


# Insert Products to MongoDB
def index(product):
    productExist = mondb.find_one({'_id': product[0]})

    if productExist is None:
            mycon = mydb.connect(myConfig['host'], myConfig['username'],
                                 myConfig['password'], myConfig['db'])
            iProduct = {'_id': int(product[0]),
                        'sku': product[3],
                        'name': product[4],
                        'long_desc': product[2],
                        'short_desc': product[1],
                        'brand': {'id': int(product[5]), 'name': product[6]},
                        'style': {'id': int(product[10]), 'name': product[11]},
                        'category': {'id': int(product[7]), 'name': product[8],
                                     'parent': int(product[9])},
                        'in_stock': 1,
                        'is_tagged': 0,
                        'shop_windows': []
                        }
            core = 0
            accent = 0

            iProduct['images'] = productImages(product[0], mycon)

            attributes = productAttributes(product[0], mycon)

            for attribute in attributes.iterkeys():
                iProduct[attribute] = attributes[attribute]

            prices = productPrices(product[0], mycon)
            iProduct['currency'] = int(prices['currency'])
            iProduct['prices'] = prices['prices']

            iProduct['sale_price'] = prices['prices']['sale']

            iProduct['concept_attributes'] = productCAs(product[0], mycon)

            cas = []

            for ca in iProduct['concept_attributes']:
                cas.append(ca['id'])
                if ca['weightage'] is 10:
                    core = ca['id']
                elif ca['weightage'] is 5:
                    accent = ca['id']

            iProduct['color_stories'] = productCSs(product[0], mycon)

            if iProduct['color_stories'] and iProduct['concept_attributes']:
                iProduct['is_tagged'] = 1

            if iProduct['concept_attributes']:
                iProduct['shop_windows'] = productSWs(cas, int(product[10]),
                                                      mycon)

            lev = list(itt.repeat(0, 20))
            lev[core-1] = 10
            lev[accent-1] = 5
            lev = tuple(lev)

            upvcC = upvcCombo(lev)
            iProduct['upvc'] = upvcC

            mondb.insert(iProduct)

            print 'Product: ' + str(product[0])+' indexed'

    else:
        exit()
        print 'Product: ' + str(product[0])+' already exists'

    return True


# Fetch Products from MySQL
def fetchproducts():
    mycon = mydb.connect(myConfig['host'], myConfig['username'],
                         myConfig['password'], myConfig['db'])
    with mycon:

        cur = mycon.cursor()
        cur.execute("SELECT p.id, p.short_desc, p.long_desc, p.sku, p.name, \
                    p.brand_id, b.brand_name, p.category_id, \
                    c.category_name, c.parent_category, \
                    b.style_id, b.style_name \
                    FROM products as p \
                    LEFT JOIN brands as b ON p.brand_id = b.id \
                    LEFT JOIN product_categories as c \
                    ON c.id = p.category_id ")
        products = cur.fetchall()
        return products


# Fetch Product Images
def productImages(id, mycon):
    with mycon:

        cur = mycon.cursor()
        cur.execute("SELECT i.path, i.type, i.label FROM product_images as i \
                    WHERE i.product_id =" + str(id))
        images = cur.fetchall()
        r = []
        for image in images:

            r.append({'path': image[0], 'type': image[1], 'label': image[2]})
        return r


# Fetch Product Attributes
def productAttributes(id, mycon):
    with mycon:

        cur = mycon.cursor()
        cur.execute("SELECT a.attribute_key, av.value, av.id \
                    FROM product_attribute_values as av \
                    LEFT JOIN  product_attributes as a \
                    ON a.id = av.attribute_id \
                    WHERE av.product_id =" + str(id))
        attributes = cur.fetchall()
        r = {}
        for attribute in attributes:
            if str(attribute[0]) == 'size':
                if 'size' not in r.keys():
                    r['size'] = []
                temp = rectify(id, attribute[2], attribute[1], mycon)

                if temp is not None:

                    if isinstance(temp, list):
                        for te in temp:
                            if te not in r['size']:
                                r['size'].append(te)
                    elif isinstance(temp, str):
                        if temp not in r['size']:
                            r['size'].append(temp)

            else:
                r[attribute[0]] = attribute[1]
        return r


# Fetch Product Prices
def productPrices(id, mycon):
    with mycon:

        cur = mycon.cursor()
        cur.execute("SELECT p.type, p.value, p.currency_id \
                    FROM product_prices as p \
                    WHERE p.product_id =" + str(id))
        prices = cur.fetchall()
        r = {'currency': 0, 'prices': {}}
        i = 0
        for price in prices:
            r['currency'] = price[2]
            r['prices'][price[0]] = str(price[1])
            i += 1
        return r


# Fetch tagged Attributes
def productCAs(id, mycon):
    with mycon:

        cur = mycon.cursor()
        cur.execute("SELECT ca.concept_attribute_id, ca.weightage \
                    FROM concept_attribute_product as ca \
                    WHERE ca.product_id =" + str(id))
        cas = cur.fetchall()
        r = []
        for ca in cas:
            r.append({'id': int(ca[0]), 'weightage': int(ca[1])})
        return r


# Fetch Tagged Color Sories
def productCSs(id, mycon):
    with mycon:

        cur = mycon.cursor()
        cur.execute("SELECT cs.id, cs.story_name \
                    FROM color_story_product as csp \
                    LEFT JOIN color_stories as cs \
                    ON cs.id = csp.color_story_id \
                    WHERE csp.product_id =" + str(id))
        css = cur.fetchall()
        r = []
        for cs in css:
            r.append({'id': int(cs[0]), 'name': cs[1]})
        return r


# Get Shop Windows
def productSWs(cas, style, mycon):
    with mycon:

        cur = mycon.cursor()
        cur.execute("SELECT sw.id,  sw.sw_name, sw.image_path \
                    FROM shop_windows as sw \
                    WHERE sw.style_id = " + str(style) + " \
                    AND sw.attribute_id in " + str(tuple(cas)))
        sws = cur.fetchall()

        r = []
        for sw in sws:
            r.append({'id': int(sw[0]), 'name': sw[1], 'image':  sw[2]})
        return r


# Get UPVC combination
def upvcCombo(lev):
    r = {}
    r = []
    for p in perm:
        r.append(upvc(lev, p))
    return r


# Calculate UPVC
def upvc(lev, l):
    r = {}
    f = list(itt.repeat(1, 20))
    i = 3
    for x in l:
        f[x-1] = i*15
        i = i - 1

    r['-'.join(itt.imap(str, l))] = sum(itt.imap(operator.mul, lev, f))
    return r


# Rectify/Purify Product Attribute 'Size'
def rectify(pid, av_id, value, mycon):
    r = value.split(',')
    if len(r) > 1:
        with mycon:
            cur = mycon.cursor()
            cur.execute('DELETE FROM product_attribute_values \
                        WHERE id = ' + str(av_id))
        temp = []
        for size in r:
            if '\xa3' not in size:
                temp.append(size)
                sql = 'INSERT INTO product_attribute_values \
                        (product_id, attribute_id, value) \
                        values (' + str(pid) + ", 9, '" + str(size) + "' )"
                cur.execute(sql)

        return temp

    elif '\xa3' in value:
        with mycon:
            cur = mycon.cursor()
            cur.execute('DELETE FROM product_attribute_values WHERE id = '
                        + str(av_id))
        return None
    else:
        return value


# Main Function
def main():
    products = fetchproducts()

    pool = mp.Pool(threads)
    for product in products:
        pool.apply_async(index, args=(product,))
    pool.close()
    pool.join()

# If you do not want MultiThreading uncomment the following line and comment
# the call of main function
# products = fetchproducts()

# for product in products:
#     index(product)

main()
