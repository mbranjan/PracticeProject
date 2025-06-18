# 1. Maximum Sum Subarray of size K.

# lst = [2, 3, 5, 2, 4, 8, 3, 5, 9, 7]
#
# i = 0
# j = 0
# k = 3
# ans = 0
# sums = 0
# while j < len(lst):
#     sums = sums + lst[j]
#     if j - i + 1 < k:
#         j = j + 1
#     elif j - i + 1 == k:
#         ans = max(sums, ans)
#         sums = sums - lst[i]
#         i = i + 1
#         j = j + 1
#
# print(ans)

##################################################################################################################
##################################################################################################################

# 2. First Negative Number in every Window of Size K
# lst = [3, 5, -7, 1, 3, -5, -5, -2, 5, 8, 9]
# i, j, k = 0, 0, 3
# temp = []
# ans = []
# while j < len(lst):
#     if lst[j] < 0:
#         temp.append(lst[j])
#     if j - i + 1 < k:
#         j = j + 1
#     elif j - i + 1 == k:
#         if len(temp) != 0:
#             ans.append(temp[0])
#             if lst[i] == temp[0]:
#                 temp.pop(0)
#             i = i + 1
#             j = j + 1
#         else:
#             ans.append(0)
#             i = i + 1
#             j = j + 1
# print(ans)

##################################################################################################################
##################################################################################################################

# 3. Count Occurrences Of Anagrams

# txt = "forxxorfxdofrof"
# pat = "for"
#
# # Input:
# # txt = "aabaabaa"
# # pat = "aaba"
# # Output: 4
# lst = list(txt)
# i, j = 0, 0
# k = len(pat)
# dct = {}
# for ch in pat:
#     if ch in dct:
#         dct[ch] = dct[ch] + 1
#     else:
#         dct[ch] = 1
# count = len(dct)
# ans = 0
# temp = ""
# while j < len(lst):
#     if lst[j] in pat:
#         dct[lst[j]] -= 1
#         if dct[lst[j]] == 0:
#             count = count - 1
#     if j - i + 1 < k:
#         j = j + 1
#     elif j - i + 1 == k:
#         if count == 0:
#             ans = ans + 1
#         if lst[i] in pat:
#             dct[lst[i]] += 1
#             if dct[lst[i]] > 0:
#                 count = count + 1
#         i = i + 1
#         j = j + 1
#
# print(ans)

##################################################################################################################
##################################################################################################################

# # 4. Maximum of all subarrays of size k
#
# lst = [2, 5, 3, -2, 4, -5, 7, 1, 4, 2, 12]
# # lst = [1, 3, -1, -3, -5, 3, 6, 7]
# k = 3
#
# i, j = 0, 0
# temp=[]
# ans =[]
# while j < len(lst):
#     x = len(temp) - 1
#     while len(temp) > 0 and lst[j] > temp[x]:
#         temp.pop(x)
#         x = x - 1
#
#     temp.append(lst[j])
#
#     if j - i + 1 < k:
#         j = j + 1
#     elif j - i + 1 == k:
#         ans.append(temp[0])
#         if lst[i] == temp[0]:
#             temp.pop(0)
#         i = i + 1
#         j = j + 1
#
# print(ans)


##################################################################################################################
##################################################################################################################

# # 5. Largest Subarray of sum K
# lst = [4, 1, 1, 1, 0, 0, 2, 3, 5]
# i, j = 0, 0
# k = 4
# summ = 0
# # start_pos = 0
# # end_pos = 0
# ans = 0
# while j < len(lst):
#     summ = summ + lst[j]
#
#     if summ < k:
#         print("i: " + str(i))
#         print("j: " + str(j))
#         print("sum: " + str(summ))
#         j = j + 1
#     elif summ == k:
#         print("i: " + str(i))
#         print("j: " + str(j))
#         print("sum: " + str(summ))
#         size = j - i + 1
#         ans = max(ans, size)
#         j = j + 1
#     else:
#         while summ > k:
#             print("i: " + str(i))
#             print("j: " + str(j))
#
#             print("sum: "+ str(summ))
#             summ = summ - lst[i]
#             i = i + 1
#             j = j + 1
#
# print(ans)


################################################
#  Longest Substring With K Unique Characters  #
################################################
# lst = ["a", "a", "b", "a", "a", "c", "b", "e", "b", "e", "b", "e", "b"]
# k = 3
#
# i = 0
# j = 0
# dct = {}
# ans = 0
# while j < len(lst):
#     if lst[j] in dct:
#         dct[lst[j]] += 1
#     else:
#         dct[lst[j]] = 1
#     if len(dct) < k:
#         j = j + 1
#     elif len(dct) == k:
#         size = j - i + 1
#         ans = max(size, ans)
#         j = j + 1
#     else:
#         while len(dct) > k:
#             if lst[i] in dct:
#                 dct[lst[i]] -= 1
#                 if dct[lst[i]] == 0:
#                     dct.pop(lst[i])
#             i = i + 1
#         j = j + 1
# print(ans)


#######################################################
# Longest Substring With Without Repeating Characters #
#######################################################

lst = ["p", "w", "w", "k", "e", "w", "z", "w"]
i = 0
j = 0
