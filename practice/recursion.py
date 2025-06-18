def print_name(cnt, numb):
    if cnt > numb:
        return
    print("My name is bhakti -- " + str(cnt))

    print_name(cnt + 1, numb)


def print_num_asc(start, end):
    if start > end:
        return
    print(start)
    print_num_asc(start + 1, end)


def print_num_asc_bktrng(start, end):
    if start < 1:
        return
    print_num_asc_bktrng(start - 1, end)
    print(start)


def print_num_desc(start, end):
    if start > end:
        return
    print(end)
    print_num_desc(start, end - 1)


def print_num_desc_bktrng(start, end):
    if start > end:
        return

    print_num_desc_bktrng(start + 1, end)
    print(start)


def total_sum(number):
    if number == 0:
        return 0

    return number + total_sum(number - 1)


def factorial(number):
    if number == 0:
        return 1
    return number * factorial(number - 1)


def reverse_array(start, length):
    if start > length / 2:
        return

    # temp = lst[start]
    # lst[start] = lst[end]
    # lst[end] = temp
    lst[start], lst[length - start - 1] = lst[length - start - 1], lst[start]
    reverse_array(start + 1, length)


def check_palindrome(lst, start, length):
    if start > length / 2:
        return "It is a palindrome"

    if lst[start] == lst[length - start - 1]:
        return check_palindrome(lst, start + 1, length)
    else:
        return "It is not a palindrome"


def fibonacci(n):
    if n <= 1:
        return n
    last = fibonacci(n - 1)
    second_last = fibonacci(n - 2)
    return last + second_last
# 0 1 1 2 3 5 8 13 21 34 55 89

if __name__ == "__main__":
    # print_name(1, 10)
    # print_num_asc(1, 10)
    # print_num_desc(1, 10)
    # print_num_asc_bktrng(10, 10)
    # print_num_desc_bktrng(1, 10)
    # print(total_sum(24))
    # print(factorial(5))

    lst = [1, 2, 3, 4, 5, 10, 7]
    # reverse_array(0, len(lst))
    # print(lst)

    # lst = "katak"
    # lst = [1, 2, 3, 2, 1]
    # print(str(check_palindrome(lst, 0, len(lst))))

    print(fibonacci(11))

    lst = [0,1]
    while n < 5:
        a = lst[i]
