mydict = {
    "research": "a detailed study of a subject, especially in order to discover (new) information or reach a (new) understanding",
    "integrity": "the quality of being honest and having strong moral principles that you refuse to change",
    "access": "the method or possibility of getting near to a place or person",
    "influence": "the power to have an effect on people or things, or a person or thing that is able to do thi"
}

# print(mydict.keys())
#
# print("Please enter the word you want to search: ")
# word = input()
#
# print("Meaning of the word {} is: ".format(word.strip()))
# print(mydict[word.strip()])


# print("Enter your age")
# age = int(input())
#
# if (age > 100 or age < 0):
#     print("Please enter a valid age between 0 to 100 : ")
#     age = int(input())
#
# if age > 18:
#     print("yes, you can drive")
# elif age < 18:
#     print("No, you can't drive")
# else:
#     print("We can't decide, Please visit physically.")

f = open("output/file.txt", "a")
# x = f.read()
f.write("add this line \n"*5)


# print(x)

f.close()
