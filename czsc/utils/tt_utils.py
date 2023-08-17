
def CLEANING_TACTICS(data, max, min):
    into = data[0][-max:]
    index = 0
    for tem in into:
        if tem > 0:
            index += 1
    if index >= min:
        print(f"当前证券码含有关键指标数为: {index},符合条件入选\n")
        return True
    else:
        print(f"当前证券码含有关键指标数为: {index},不符合条件放弃\n")

