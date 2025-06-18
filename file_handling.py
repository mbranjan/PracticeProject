# num = int(input("Enter the number of rows: "))
# type = input("""Enter the style:
# f for forward and
# b for backward """)
# f = open("output/file.txt", "w")
# if type == "f":
#     itr = 1
#     while itr <= num:
#         f.write("*" * itr + "\n")
#         # print("*" * itr)
#         itr += 1
# else:
#     itr = num
#     while itr >= 1:
#         f.write("*" * itr + "\n")
#         # print("*" * itr)
#         itr -= 1
# f.close()


strng1 = "one two three four five six seven eight nine ten"


# def func(strng):
#     i = 0
#     final_list = []
#     lst = strng.split(" ")
#     for i in range(len(lst)):
#         if i % 2 == 0:
#             final_list.append(lst[i])
#         else:
#             final_list.append(lst[i][::-1])
#         i += 1
#     for i in final_list:
#         print(i, end=" ")
#
#
# func(strng1)


# x = 89
# def harry():
#     x =20
#     def rohan():
#         global x
#         x =70
#         print("inside rohan: ",x)
#     print("before rohan",x)
#     rohan()
#     print("after rohan", x)
# harry()
# print(x)