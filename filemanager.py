import os


def directory_iterate(directory_list):
    filepath_list = []
    for directory in directory_list:
        for dir_item in os.listdir(directory):
            dir_item_path = os.path.join(directory, dir_item)
            filepath_list.append(dir_item_path)
    return filepath_list
