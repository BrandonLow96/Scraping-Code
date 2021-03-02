Test = ["Things1", "Things2", "Things3", "Things4", "Things5"]

with open('C:/Users/BRODMAN/Documents/Code/Python/Test Space/Folder 1/Test 1.txt', mode='wt', encoding='utf-8') as myfile:
    myfile.write('\n'.join(Test))

    with open('C:/Users/BRODMAN/Documents/Code/Python/Test Space/Folder 1/Test 1.txt') as f:
    Testing = [line.rstrip() for line in f]
