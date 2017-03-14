if __name__ == '__main__':
    import os
    path = '/search/image/attr_result/'
    poi_set = set()
    for file_name in os.listdir(path):
        poi_id = file_name.split('_')[0]
        poi_set.add(poi_id)
    print((len(poi_set)))