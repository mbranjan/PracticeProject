# ###############################################
# Maximum Sum Subarray of size K
# ###############################################

# num_list = [2, 4, 5, 3, 6, 8, 1, 2, 4]
#
# i, j = 0, 0
#
# k = 3
# csum = 0
# msum = 0
#
# while (j < len(num_list)):
#     csum = csum + num_list[j]
#     if (j - i + 1 < k):
#         j = j + 1
#     elif (j - i + 1 == k):
#         msum = max(csum, msum)
#         csum = csum - num_list[i]
#         i = i + 1
#         j = j + 1
# print(msum)


# #################################################### ###################################################
# First negative integer in every window of size k
# #################################################### ###################################################

# lst = [12, -1, -7, 8, -15, 30, 16, 28,30,11,-2]
#
# i = 0
# j = 0
# flag = 0
# k = 3
# oplist = []
#
# while (i < len(lst) - 2):
#     # print("inside 1st loop value of i,j:")
#     # print(i,j)
#     j = i
#     while (j < i + k):
#         # print("inside 2nd loop value of i,j:")
#         # print(i, j)
#         flag = 0
#         if lst[j] < 0:
#             oplist.append(lst[j])
#             flag = 1
#             break
#         j = j + 1
#     if (flag == 0):
#         oplist.append(0)
#     i = i + 1
# print(oplist)

# lst = [12, -1, -7, 8, -15, 30, 16, 28, 30, 11, -2]
#
# i = 0
# j = 0
# k = 3
# intlist = []
# oplist=[]
# # print(i,j)
# while (j < len(lst)):
#     # print(i,j)
#     if (lst[j] < 0):
#         # print("inside first if", str(i), str(j))
#         intlist.append(lst[j])
#     if (j - i + 1 < k):
#         # print("inside second if",str(i), str(j))
#         j += 1
#     elif (j - i + 1  == k):
#         # print("inside else if", str(i), str(j))
#         if (len(intlist) == 0 ):
#             oplist.append(0)
#             j += 1
#             i += 1
#         else:
#             oplist.append(intlist[0])
#             if(lst[i] == intlist[0]):
#                 intlist.pop(0)
#             j += 1
#             i += 1
#
# print(oplist)


# #################################################### ###################################################
# Find maximum in every window of size k
# #################################################### ###################################################

# lst = [1, 3, -1, -3, 4,4, 6, 7]
#
# i = 0
# j = 0
# k = 3
# tmp=[]
# tmp.append(lst[0])
# ans =[]
# while (j < len(lst)):
#     if lst[j] > tmp[0]:
#         tmp.clear()
#         tmp.append(lst[j])
#     else:
#         tmp.append(lst[j])
#
#     if (j - i + 1 < k):
#         j += 1
#     elif (j - i + 1 == k):
#         ans.append(tmp[0])
#         if lst[i] == tmp[0]:
#             tmp.pop(0)
#         i += 1
#         j += 1
# print(lst)
# print(ans)


# #################################################### ###################################################
# Largest Subarray of sum k
# #################################################### ###################################################

# lst = [4, 1, 1, 1, -2, 2, 3, 5]
#
# i = 0
# j = 0
# k = 5
# csum = 0
# ans = 0
#
# while (j < len(lst)):
#     csum = csum + lst[j]
#     if (csum < k):
#         j += 1
#     elif (csum == k):
#         if (j - i + 1 > ans):
#             ans = j - i + 1
#         j += 1
#     elif (csum > k):
#         while (csum > k):
#             csum = csum - lst[i]
#             i += 1
#         j += 1
# print(ans)


# ###############################################
# Maximum Sum Subarray of size K
# ###############################################

# lst = [1, 3, 5, 2, 3, 7, 3, 9, 1, 4, 6, 8]
# k = 4
# sm = 0
# i = 0
# j = 0
# max_sum = 0
#
# while j < len(lst):
#     sm = sm + lst[j]
#     if (j - i + 1) < k:
#         j += 1
#     elif (j - i + 1) == k:
#         if max_sum < sm:
#             max_sum = sm
#         sm = sm - lst[i]
#         i += 1
#         j += 1
#
# print(max_sum)


# ###############################################
# First Negative Number in every Window of Size K
# ###############################################
# Brute Force Approach
# ---------------------
# lst = [12, -1, -7, 8, -15, 30, 16, 28]
# k = 3
# i = 0
# j = i + k - 1
# # x = 0
# res = []
#
# while j < len(lst):
#     flag = 0
#     x = i
#     while x <= j:
#         if lst[x] < 0:
#             res.append(lst[x])
#             flag = 1
#             break
#         # print(res)
#         x += 1
#     if flag == 0:
#         res.append(0)
#     # print(i, j)
#     i += 1
#     j += 1
#
# print(res)

# Sliding Window Approach
# ------------------------
# lst = [12, -1, -7, 8, -15, 30, 16, 28,-3]
# k = 3
# i = 0
# j = 0
#
# neg_num = []
# ans = []
#
# while j < len(lst):
#     if lst[j] < 0:
#         neg_num.append(lst[j])
#
#     if (j - i + 1) < k:
#         j = j + 1
#
#     elif (j - i + 1) == k:
#         # print(i, j)
#         if len(neg_num) > 0:
#             ans.append(neg_num[0])
#             if lst[i] == neg_num[0]:
#                 # neg_num.remove(lst[i])
#                 neg_num.pop(0) # both remove and pop gives same answer
#             i = i + 1
#             j = j + 1
#         else:
#             ans.append(0)
#             i = i + 1
#             j = j + 1
#     print(neg_num)
#
# print(ans)

####################################################
#  Count Occurrences Of Anagrams | Sliding Window  #
####################################################

# input:
# txt = forxxorfxdofr
# pat = for
# Output: 3
# Explanation: for, orf and ofr appears
# in the txt, hence answer is 3.
# Example 2:

# Input:
# txt = aabaabaa
# pat = aaba
# Output: 4

# def getCountOFAnagram(string, pattern):
#     n = len(string)
#     start = 0
#     end = 0
#     d = dict()
#     ans = 0
#     k = len(pattern)
#     for i in pattern:
#         d[i] = d.get(i, 0) + 1
#     count = len(d)
#     while end < n:
#         if string[end] in d:
#             d[string[end]] -= 1
#             if d[string[end]] == 0:
#                 count -= 1
#         if end - start + 1 < k:
#             end += 1
#         elif end - start + 1 == k:
#             if count == 0:
#                 ans += 1
#             if string[start] in d:
#                 d[string[start]] += 1
#                 if d[string[start]] == 1:
#                     count += 1
#             start += 1
#             end += 1
#     return ans
#
# if __name__ == '__main__':
#     # print(countOfAnagram("forxxorfxdofr", "for"))
#     print(getCountOFAnagram("aabaabaa", "aaba"))


######################################
# Maximum of all subarrays of size k
######################################

# nums = [1, 3, -1, -3, 5, 3, 6, 7]
# k = 3
# i = 0
# j = 0
# ans = []
# temp = []
# while j < len(nums):
#     x = len(temp) - 1
#     while len(temp) > 0 and nums[j] > temp[x]:
#         temp.pop(x)
#         x = x - 1
#     temp.append(nums[j])
#
#     if j - i + 1 < k:
#         j += 1
#
#     elif j - i + 1 == k:
#         ans.append(temp[0])
#         if nums[i] == temp[0]:
#             temp.pop(0)
#         i += 1
#         j += 1
#
# print(ans)
# expected ans = [3, 3, 5, 5, 6, 7]


###########################################
###########################################
#  VARIABLE WINDOW SIZE - SLIDING WINDOW  #
###########################################
###########################################


###########################################
# Largest Subarray of sum K
###########################################

# nums = [4, 1, 1, 1, 2, 3, 5]
# k = 5
# i = 0
# j = 0
# sum = 0
# ans = 0
# while j < len(nums):
#     sum = sum + nums[j]
#     if sum < k:
#         j = j + 1
#     elif sum == k:
#         size = j - i + 1
#         if ans < size:
#             ans = size
#         j = j + 1
#     else:
#         while sum > k:
#             sum = sum - nums[i]
#             i = i + 1
#         j = j + 1
# print(ans)


################################################
#  Longest Substring With K Unique Characters  #
################################################
# lst = ["a", "a", "b", "a", "c", "b", "e", "b", "e", "b", "e"]
# k = 3
# i = 0
# j = 0
# dct = {}
# ans = 0
#
# while j < len(lst):
#     if lst[j] in dct.keys():
#         dct[lst[j]] += 1
#     else:
#         dct[lst[j]] = 1
#     if len(dct) < k:
#         j = j + 1
#     elif len(dct) == k:
#         size = j - i + 1
#         if ans < size:
#             ans = size
#         j = j + 1
#     else:
#         while len(dct) > k:
#             dct[lst[i]] -= 1
#             if dct[lst[i]] == 0:
#                 dct.pop(lst[i])
#             i = i + 1
#         j = j + 1
# print(ans)


#######################################################
# Longest Substring With Without Repeating Characters #
#######################################################

# lst = ["p", "w", "w", "k", "e", "w","z","w"]
# i = 0
# j = 0
# dct = {}
# cnt = 0
# ans = 0
#
# while j < len(lst):
#     if lst[j] in dct.keys():
#         dct[lst[j]] += 1
#     else:
#         dct[lst[j]] = 1
#
#     if (j - i + 1) == len(dct):
#         size = j - i + 1
#         if ans < size:
#             ans = size
#         j = j + 1
#     elif (j - i + 1) > len(dct):
#         while len(dct) < (j - i + 1):
#             dct[lst[i]] -= 1
#             if dct[lst[i]] == 0:
#                 dct.pop(lst[i])
#             i = i + 1
#         j = j + 1
# print(ans)



###########################################################
# Minimum Window Substring | Variable Size Sliding Window #
###########################################################

# def minimumSubstringWindow(strng, t):
#     count_map = dict()
#     n = len(strng)
#
#     for ch in t:
#         if ch in count_map:
#             count_map[ch] += 1
#
#         else:
#             count_map[ch] = 1
#
#     start = 0
#     end = 0
#     map_size = len(count_map)
#     min_window_len = 1000000007
#     maxStart = 0
#     maxend = 0
#
#     while end < n:
#         if strng[end] in count_map:
#             count_map[strng[end]] -= 1
#
#             if count_map[strng[end]] == 0:
#                 map_size -= 1
#
#         while map_size == 0:
#             min_window_len = min(min_window_len, (end - start + 1))
#             maxStart = start  # substring start index
#             maxend = end + 1  # substring end index
#
#             ch_start = strng[start]
#             if ch_start in count_map:
#                 count_map[ch_start] += 1
#                 if count_map[ch_start] > 0:
#                     map_size += 1
#
#             start += 1
#
#         end += 1
#
#     print("substring start index {} and end index {}".format(maxStart, maxend))
#     # return min_window_len
#     return strng[maxStart:maxend]
#
# if __name__ == '__main__':
#     print(minimumSubstringWindow("A", "A"))
#     print(minimumSubstringWindow("ADOBECCCODEBANC", "ABC"))
