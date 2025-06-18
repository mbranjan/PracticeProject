# ##################
# FIBONACCI SERIES #
# ##################

# BY USING LOOP
# -------------
# fib_len = int(input("Enter the length of the Fibonacci series: "))
#
# 0, 1, 1, 2, 3, 5, 8
#
# first_num = 0
# second_num = 1
# n = 1
# fibo_list = [0, 1]
# while n < fib_len - 1:
#     fibo_num = first_num + second_num
#     first_num = second_num
#     second_num = fibo_num
#     n += 1
#     fibo_list.append(fibo_num)
#
# print(fibo_list)

# BY USING RECURSION
# -------------------
# fib_len = int(input("Enter the length of the Fibonacci series: "))
# first_num = 0
# second_num = 1
# n = 2
# fibo_list = [0, 1]
# list1 = [0]
#
#
# def fibo(first_num, second_num):
#     global n
#     if fib_len == 1:
#         return list1
#     elif n > fib_len - 1:
#         return fibo_list
#     else:
#         fibo_num = first_num + second_num
#         n += 1
#         fibo_list.append(fibo_num)
#         fibo(second_num, fibo_num)
#         return fibo_list
#
#
# print(fibo(first_num, second_num))


# ##################
#     FACTORIAL    #
# ##################

# fct_num = int(input("Please enter the number to find factorial: "))
#
# def fact(n):
#     if n == 1:
#         return 1
#     else:
#         fct = n * fact(n-1)
#         return fct
#
# print(fact(fct_num))

# ########################
# FACTORIAL OF FACTORIAL #
# ########################

# 5 !! = 4 ! * 3! * 2 ! * 1 !
# 120 * 24* 6* 2
def fact(n):
    if n == 1 or n == 0:
        return 1
    else:
        return n * fact(n - 1)


fct_num = int(input("Please enter the number to find factorial: "))

fct_of_fct = 1
while fct_num > 0:
    fct_of_fct = fct_of_fct * fact(fct_num)
    fct_num -= 1

print(fct_of_fct)

