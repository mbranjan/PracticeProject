action = input(""" What do you want to do?
    R -> for Retrieve
    L -> for Log
""")

name = input(""" Enter the name of the client:
    1 for Rohan
    2 for Rahul
    3 for Rakesh 
 """)

def get_time():
    import datetime
    return datetime.datetime.now()


def logging(name):
    activity = input(""" What do you want to log? 
        d for diet
        e for exercise
        """)
    if name == "1" and activity == "d":
        print("condition1")
        file_name = "health/rohan_diet.txt"
    elif name == "1" and activity == "e":
        print("condition2")
        file_name = "health/rohan_exercise.txt"
    elif name == "2" and activity == "d":
        print("condition3")
        file_name = "health/rahul_diet.txt"
    elif name == "2" and activity == "e":
        file_name = "health/rahul_exercise.txt"
    elif name == "3" and activity == "d":
        file_name = "health/rakesh_diet.txt"
    else:
        file_name = "health/rakesh_exercise.txt"

    print("name is : "+ name)
    print("activity is : " + activity)
    details = input("Enter the details to be logged: ")
    with open(file_name, "a") as f:
        f.write("[ {} ] : ".format(str(get_time())))
        f.write(details + "\n")
        print("Details added"+file_name)


def retrieve(name):
    client_dict = {"1":"Rohan", "2":"Rahul", "3":"Rakesh"}
    if name == "1":
        file1 = "health/rohan_diet.txt"
        file2 = "health/rohan_exercise.txt"

    elif name == "2":
        file1 = "health/rahul_diet.txt"
        file2 = "health/rahul_exercise.txt"
    else:
        file1 = "health/rakesh_diet.txt"
        file2 = "health/rakesh_exercise.txt"

    print("Diet details of {}: ".format(client_dict.get(name)))
    with open(file1) as f:
        print(f.read())
    print("Exercise details of {}: ".format(client_dict.get(name)))
    with open(file2) as f:
        print(f.read())


if action.lower() == "l":
    logging(name)

else:
    retrieve(name)
