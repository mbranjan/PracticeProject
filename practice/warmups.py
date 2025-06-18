# QUESTION - 1
#####################################################################################################################
"""Given an integer array nums, return true if any value appears at least twice in the array, and return false if every element is distinct.
Examples

Example 1:
Input: nums= [1, 2, 3, 4]
Output: false
Explanation: There are no duplicates in the given array.

Example 2:
Input: nums= [1, 2, 3, 1]
Output: true
Explanation: '1' is repeating.

Example 3:
Input: nums= [3, 2, 6, -1, 2, 1]
Output: true
Explanation: '2' is repeating."""

# # lst = [1, 2, 3, 4]
# lst = [1, 2, 3, 1]
# # lst = [3, 2, 6, -1, 2, 1]
# flag = 0
# i = 0
# while i < len(lst) - 1:
#     j = i + 1
#     while j < len(lst):
#         if lst[i] == lst[j]:
#             flag = 1
#             break
#         else:
#             j = j + 1
#     i = i + 1
#
# if flag == 1:
#     print("true - duplicate is there")
# else:
#     print("no duplicates")


# QUESTION - 2
#####################################################################################################################
""" A pangram is a sentence where every letter of the English alphabet appears at least once.
Given a string sentence containing English letters (lower or upper-case), return true if sentence is a pangram, or false otherwise.
Note: The given sentence might contain other characters like digits or spaces, your solution should handle these too.

Example 1:
    Input: sentence = "TheQuickBrownFoxJumpsOverTheLazyDog"
    Output: true
    Explanation: The sentence contains at least one occurrence of every letter of the English alphabet either in lower or upper case.

Example 2:
    Input: sentence = "This is not a pangram"
    Output: false
    Explanation: The sentence doesn't contain at least one occurrence of every letter of the English alphabet.

Constraints:
    1 <= sentence.length <= 1000
    sentence consists of lower or upper-case English letters. """

# import string
# # sentence = "TheQuickBrownFoxJumpsOverTheLazyDog"
# sentence = "This is not a pangram"
# # comp_string = string.ascii_lowercase
# # print(comp_string) ==>  abcdefghijklmnopqrstuvwxyz
#
# comp_string = "abcdefghijklmnopqrstuvwxyz"
# temp = set()
#
# for char in sentence:
#     if char.lower() in comp_string:
#         temp.add(char.lower())
#
# if len(temp) == 26:
#     print("it is a pangram")
# else:
#     print("It is not a pangram")


# QUESTION - 3
#####################################################################################################################
""" Given a string s, reverse only all the vowels in the string and return it.
The vowels are 'a', 'e', 'i', 'o', and 'u', and they can appear in both lower and upper cases, more than once.

Example 1:
Input: s= "hello"
Output: "holle"

Example 2:
Input: s= "AEIOU"
Output: "UOIEA"

Example 3:
Input: s= "DesignGUrus"
Output: "DusUgnGires"  
"""

# s = "DesignGUrus"
# s= "AEIOU"
# s = "hello"
# temp = []
# ans = []
# for char in s:
#     if char in 'AEIOU' or char in 'aeiou':
#         temp.append(char)
#
# j = len(temp) - 1
# for ch in s:
#     if ch in 'AEIOU' or ch in 'aeiou':
#         ans.append(temp[j])
#         j = j - 1
#     else:
#         ans.append(ch)
# print("".join(ans))


# QUESTION - 4
#####################################################################################################################
""" A phrase is a palindrome if, after converting all uppercase letters into lowercase letters and removing all non-alphanumeric characters, it reads the same forward and backward. Alphanumeric characters include letters and numbers.

Given a string s, return true if it is a palindrome, or false otherwise.

Example 1:

Input: sentence = "A man, a plan, a canal, Panama!"
Output: true
Explanation: "amanaplanacanalpanama" is a palindrome.

Example 2:

Input: sentence = "Was it a car or a cat I saw?"
Output: true
 """

# # sentence = "A man, a plan, a canal, Panama!"
# sentence = "Was it a car or a cat I saw?"
# temp = []
# for ch in sentence:
#     if ch.isalnum():
#         temp.append(ch.lower())
# print(temp)
# flag = 0
# i = 0
# j = len(temp) - 1
# while i <= j:
#     if temp[i] == temp[j]:
#         i = i + 1
#         j = j - 1
#     else:
#         flag = 1
#         break
# if flag == 1:
#     print("it is not a palindrome")
# else:
#     print("it is a palindrome")


# QUESTION - 5
#####################################################################################################################
""" Given two strings s and t, return true if t is an anagram of s, and false otherwise.
An Anagram is a word or phrase formed by rearranging the letters of a different word or phrase, typically using all the original letters exactly once.

Example 1:
    Input: s = "listen", t = "silent"
    Output: true

Example 2:
    Input: s = "rat", t = "car"
    Output: false

Example 3:
    Input: s = "hello", t = "world"
    Output: false

Constraints:
    1 <= s.length, t.length <= 5 * 104
    s and `t  """

# =============
# solution - 1
# =============
# # s, t = "listen", "silent"
# # s, t = "rat", "tar"
# s, t = "hello", "world"
# flag = 0
# dct1, dct2 = {}, {}
#
# for ch in s:
#     if ch in dct1:
#         dct1[ch] += 1
#     else:
#         dct1[ch] = 1
#
# for ch in t:
#     if ch in dct2:
#         dct2[ch] += 1
#     else:
#         dct2[ch] = 1
#
# for char in s:
#     if char in dct1 and char in dct2 and dct1[char] == dct2[char]:
#         continue
#     else:
#         flag = 1
#         break
#
# if flag == 1:
#     print(" it is not an anagram")
# else:
#     print(" it is an anagram")

# =============
# solution - 2
# =============
# s1, t1 = "listen", "silent"
# # s1, t1 = "rat", "car"
# # s1, t1 = "hello", "world"
# s = list(s1)
# t = list(t1)
# flag = 0
# if len(s) == len(t):
#     for ch in s:
#         if ch in t:
#             t.remove(ch)
#         else:
#             flag = 1
#             break
# else:
#     flag = 1
#
# if flag == 1:
#     print(" it is not an anagram")
# else:
#     print(" it is an anagram")


# QUESTION - 6
#####################################################################################################################
""" Given an array of strings words and two different strings that already exist in the array word1 and word2, return the shortest distance between these two words in the list.

Example 1:
    Input: words = ["the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"], word1 = "fox", word2 = "dog"
    Output: 5
    Explanation: The distance between "fox" and "dog" is 5 words.

Example 2:
    Input: words = ["a", "c", "d", "b", "a"], word1 = "a", word2 = "b"
    Output: 1
    Explanation: The shortest distance between "a" and "b" is 1 word """
# import sys
# i, j = -1, -1
# ans = sys.maxsize
# # word1 = "fox"
# # word2 = "dog"
# # words = ["the", "quick", "brown", "fox", "jumps", "over", "the", "lazy", "dog"]
# words = ["a", "c", "d", "b", "a"]
# word1 = "a"
# word2 = "b"
#
# for ind, word in enumerate(words):
#     # print(k, w)
#     if word == word1:
#         i = ind
#     if word == word2:
#         j = ind
#     if i != -1 and j != -1 and ans != -1:
#         ans = min(abs(i - j), ans)
# print(ans)


# QUESTION - 6
#####################################################################################################################
""" Given an array of integers nums, return the number of good pairs.

A pair (i, j) is called good if nums[i] == nums[j] and i < j.

Example 1:
    Input: nums = [1,2,3,1,1,3]
    Output: 4
    Explanation: There are 4 good pairs, here are the indices: (0,3), (0,4), (3,4), (2,5).

Example 2:
    Input: nums = [1,1,1,1]
    Output: 6
    Explanation: Each pair in the array is a 'good pair'.

Example 3:
    Input: nums = [1,2,3]
    Output: 0
    Explanation: No number is repeating.

Constraints:
    1 <= nums.length <= 100
    1 <= nums[i] <= 100 """

# # nums = [1, 2, 3, 1, 1, 3]
# # nums = [1,1,1,1]
# nums = [1, 2, 3]
# i = 0
# cnt = 0
# while i < len(nums):
#     j = i + 1
#     while j < len(nums):
#         if nums[i] == nums[j]:
#             cnt += 1
#             j += 1
#         else:
#             j += 1
#     i += 1
# print(cnt)


# QUESTION - 7
#####################################################################################################################
""" Problem Statement:
Given a non-negative integer x, return the square root of x rounded down to the nearest integer. The returned integer should be non-negative as well.
You must not use any built-in exponent function or operator.
For example, do not use pow(x, 0.5) in c++ or x ** 0.5 in python.

Example 1:
    Input: x = 8
    Output: 2
    Explanation: The square root of 8 is 2.8284, and since we need to return the floor of the square root (integer), hence we returned 2.  

Example 2:
    Input: x = 4
    Output: 2
    Explanation: The square root of 4 is 2.

Example 3:
    Input: x = 2
    Output: 1
    Explanation: The square root of 2 is 1.414, and since we need to return the floor of the square root (integer), hence we returned 1.  

Constraints:
    0 <= x <= 231 - 1
 """

# num = 15
# start = 0
# end = num
# res = -1
# while start <= end:
#     mid = (start+end) // 2
#     if mid * mid == num:
#         res = mid
#         break
#     elif mid * mid > num:
#         end = mid - 1
#     else:
#         start = mid + 1
# if res == -1:
#     res = end
# print(res)


# QUESTION - 8
#####################################################################################################################
""" Problem Statement:
Given an array of numbers sorted in ascending order and a target sum, find a pair in the array whose sum is equal to the given target.
Write a function to return the indices of the two numbers (i.e. the pair) such that they add up to the given target. If no such pair exists return [-1, -1].

Example 1:
    Input: [1, 2, 3, 4, 6], target=6
    Output: [1, 3]
    Explanation: The numbers at index 1 and 3 add up to 6: 2+4=6

Example 2:
    Input: [2, 5, 9, 11], target=11
    Output: [0, 2]
    Explanation: The numbers at index 0 and 2 add up to 11: 2+9=11

Constraints:
    2 <= arr.length <= 104
    -109 <= arr[i] <= 109
    -109 <= target <= 109
    Only one valid answer exists.
"""

# # lst = [1, 2, 3, 4, 5, 6]
# lst = [2, 5, 9, 11]
# target = 11
# flag = -1
# start = 0
# end = len(lst) - 1
# while start <= end:
#     if lst[start] + lst[end] == target:
#         flag = 0
#         print(start, end)
#         break
#     elif lst[start] + lst[end] > target:
#         end = end - 1
#     else:
#         start = start + 1
#
# if flag == -1:
#     print("No pair in the array whose sum is equal to the given target")


# QUESTION - 9
#####################################################################################################################
""" Find Non-Duplicate Number Instances (easy)

Given an array of sorted numbers, move all non-duplicate number instances at the beginning of the array in-place. 
The non-duplicate numbers should be sorted and you should not use any extra space so that the solution has constant space complexity i.e.

Move all the unique number instances at the beginning of the array and after moving return the length of the subarray that has no duplicate in it.

Example 1:
    Input: [2, 3, 3, 3, 6, 9, 9]
    Output: 4
    Explanation: The first four elements after moving element will be [2, 3, 6, 9].

Example 2:
    Input: [2, 2, 2, 11]
    Output: 2
    Explanation: The first two elements after moving elements will be [2, 11].

Constraints:
    1 <= nums.length <= 3 * 104
    -100 <= nums[i] <= 100
    nums is sorted in non-decreasing order.
"""

# lst = [2, 3, 3, 3, 6, 9, 9]
lst = [2, 2, 2, 11]
start = 0
end = 1
cnt = 0
while end < len(lst):
    while end < len(lst) and lst[start] == lst[end]:
        lst.pop(end)
        cnt = cnt + 1
    start = start + 1
    end = end + 1
    cnt = cnt + 1

print(lst)
print(len(lst))
print(cnt)
