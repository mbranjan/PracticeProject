# #################################################### ###################################################
# Find an element in array with Binary search
# #################################################### ###################################################

# num = [2, 3, 5, 7, 11, 24, 37, 98, 102, 306]
# res = -1
# ele = 5
# start = 0
# end = len(num) - 1
#
# while (start <= end):
#     mid = (start + end) // 2
#     if (ele == num[mid]):
#         res = mid
#         break
#     elif (ele < num[mid]):
#         end = mid - 1
#     else:
#         start = mid + 1
#
# if (res == -1):
#     print("Element is not present in List")
# else:
#     print(res)


# #################################################### ###################################################
# Count of an element in a sorted array with Binary search
# #################################################### ###################################################

# lst = [2, 4, 10, 10, 10, 10, 10, 10, 10, 18, 20]
#
# ele = 10
# start = 0
# end = len(lst) - 1
# res = -1
#
# while (start <= end):
#     mid = start + (end - start) // 2
#     if (ele == lst[mid]):
#         res = mid
#         end = mid - 1
#     elif (ele < lst[mid]):
#         end = mid - 1
#     else:
#         start = mid + 1
# first_place = res
#
# start = 0
# end = len(lst) - 1
# res = -1
# while (start <= end):
#     mid = start + (end - start) // 2
#     if (ele == lst[mid]):
#         res = mid
#         start = mid + 1
#     elif (ele < lst[mid]):
#         end = mid - 1
#     else:
#         start = mid + 1
# last_place = res
#
# print(first_place, last_place, last_place - first_place + 1)


# nums = [1, 2, 3, 4, 3]
# # nums= [3, 2, 6, -1, 2, 1]
# # nums= [1, 2, 3, 4]
# res = False
# i = 0
# j = 0
# flag = 0
# while i < len(nums):
#     while j < len(nums):
#         if i != j and nums[i] == nums[j]:
#             res = True
#             flag = 1
#             break
#         else:
#             # print("incrementing J to " + str(j))
#             j = j + 1
#     if flag == 1:
#         break
#     i = i + 1
#     j = 0
# print(res)

# x = "hello"
# x = "AEIOU"
# x= "DesignGUrus"
# s = list(x)
# l = []
# for i in s:
#     if i in 'aeiou' or i in 'AEIOU':
#         l.append(i)
# j = len(s) - 1
# i = 0
# while j >= 0:
#     if s[j] in 'aeiou' or s[j] in 'AEIOU':
#         s[j] = l[i]
#         i = i + 1
#     j = j - 1
# print("".join(s))


# sentence = "TheQuickBrownFoxJumpsOverTheLazyDog"
# sentence = "This is not a pangram"
# st = set()
# for i in sentence:
#     if i.lower() in "abcdefghijklmnopqrstuvwxyz":
#         st.add(i.lower())
#
# if len(st) == 26:
#     print(True)
# else:
#     print(False)


# s = "bass"
# t = "asbs"
# s1 = list(s)
# l1 = len(s1)
# t1 = list(t)
# l2 = len(t1)
# flag = 0
#
# for i in range(len(s1)):
#     for j in range(len(t1)):
#         if s1[i] == t1[j]:
#             t1.pop(j)
#             flag = flag + 1
#             break
#
# if l1 == l2 and l1 == flag:
#     print("it is an anagram")
# else:
#     print("it is not an anagram")


n = 125
start = 0
end = n
result = -1
while start <= end:
    mid = start + (end-start)//2
    if mid*mid == n:
        result = mid
        break
    elif mid*mid < n:
        start = mid + 1
    else:
        end = mid - 1
if result == -1:
    result = end

print(result)