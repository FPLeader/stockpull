def IDE2SECID(ide):
    if ide[0] == 'B':
        return '90' + '.' + ide[:6]
    return {'1': '1', '2': '0'}.get(ide[6:], '') + '.' + ide[:6]


def IDE2SECID_FORX(ide):
    return '119.' + ide[:6]


def ID62IDE(id6):
    return id6 + {'0': '2', '3': '2', '6': '1'}.get(id6[0], '')


def IDE2SID(ide):
    if ide[0:2] == 'BK':
        return '90.' + ide
    else:
        return {'1': 'SH', '2': 'SZ'}.get(ide[6:], '') + ide[:6]


def ID62IDE_with_mk(id6='000001', mk='0'):
    return id6 + {'0': '2', '1': '1'}.get(str(mk), str(mk))


class Configuration(object):
    def __init__(self, **kwds):
        vars(self).update(kwds)

    def __repr__(self, *args, **kwargs):
        return 'Configuration(**%s)' % vars(self)

    def __str__(self, *args, **kwargs):
        return 'Configuration:\n%s' % '\n'.join('%20s = %s' % (k, v)
                                                for k, v in sorted(vars(self).items()))

    def update(self, props):
        d = props if isinstance(props, dict) else vars(props)
        vars(self).update(d)


if __name__ == '__main__':
    from pymongo import MongoClient

    # Requires the PyMongo package.
    # https://api.mongodb.com/python/current

    client = MongoClient(
        'mongodb://localhost:27017/?readPreference=primary&appname=MongoDB+Compass&directConnection=true&ssl=false')
    result = client['StockDB']['DG'].aggregate([
        {
            '$group': {
                '_id': '$orgSName',
                'count': {
                    '$count': {}
                }
            }
        }
    ])
    print(result)
    # print(IDE2SECID('0000011'))
    # print(IDE2SID('0000012'))
    # print(ID62IDE('623456'))
    # print(IDE2SID("BK0459"))
