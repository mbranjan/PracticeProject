# Input: arr[] = {4, 6, 10, 12, 18, 20}, K = 6
# Output:
# Lower bound of 6 is 6 at index 1
# Upper bound of 6 is 10 at index 2
# Input: arr[] = {4, 6, 10, 12, 18, 20}, K = 20
# Output:
# Lower bound of 20 is 20 at index 5
# Upper bound doesn’t exist

# lst = [4, 6, 10, 12, 18, 20]
# k =6

# ans = -1
# i = 0
# j = 5
#
# while i <= j:
#     mid = round(i + (j - i) // 2)
#     if k == lst[mid]:
#         ans = mid
#         break
#     elif k < lst[mid]:
#         j = mid - 1
#     else:
#         i = mid + 1
#
# if ans == -1:
#     print("Element is not present in list")
# else:
#     print(ans)


# # It returns location of x in given array arr
# def binarySearch(arr, low, high, x):
#
#     while low <= high:
#
#         mid = low + (high - low) // 2
#
#         # Check if x is present at mid
#         if arr[mid] == x:
#             return mid
#
#         # If x is greater, ignore left half
#         elif arr[mid] < x:
#             low = mid + 1
#
#         # If x is smaller, ignore right half
#         else:
#             high = mid - 1
#
#     # If we reach here, then the element
#     # was not present
#     return -1
#
#
# # Driver Code
# if __name__ == '__main__':
#     # arr = [2, 3, 4, 10, 40]
#     # x = 10
#     arr = [4, 6, 10, 12, 18, 20]
#     x =20
#
#     # Function call
#     result = binarySearch(arr, 0, len(arr)-1, x)
#     if result != -1:
#         print("Element is present at index", result)
#     else:
#         print("Element is not present in array")


# lst = [4, 6, 10, 12, 18, 20, 25]
# k = 6
#
# start = 0
# end = len(lst) - 1
# res = -1
#
# while start <= end:
#     mid = start + (end - start) // 2
#     if lst[mid] == k:
#         res = mid
#         break
#     elif lst[mid] < k:
#         start = mid + 1
#     else:
#         end = mid - 1
#
# if res == -1:
#     print("number was not found")
# else:
#     print("Lower bound is present at : " + str(res))
#     if res == len(lst) - 1:
#         print("Upper bound doesn't exist")
#     else:
#         print("Upper bound is present at : " + str(res + 1))


# lst = [1, 3, 5, 6, 9]
# k = 10
# res = -1
# start = 0
# end = len(lst) - 1
#
# while start <= end:
#     mid = start + (end - start) // 2
#     if lst[mid] == k:
#         res = mid
#         break
#     elif lst[mid] < k:
#         start = mid + 1
#     else:
#         end = mid - 1
#
# if res == -1:
#     res = start
#
# print("insert position is: " + str(res))


# n = int(input("please enter the number to find out the sqrt: "))
# # lst = range(n)
# start = 0
# end = n
# res = -1
#
#
# while start <= end:
#     mid = start + (end - start) // 2
#     if mid * mid == n:
#         res = mid
#         break
#     elif mid * mid > n:
#         end = mid - 1
#     else:
#         start = mid + 1
#
# if res == -1:
#     res = start - 1
#
# print("Answer is : " + str(res))

#
lst = [1, 2, 2, 2, 2, 2, 2, 3, 4, 5, 5, 5, 5, 67, 123, 125]
x = 5

start = 0
end = len(lst) - 1
res = 0

while start <= end:
    # mid = start + (end - start) // 2
    mid = start + (end - start) // 2
    if lst[mid] == x:
        res = mid
        end = mid - 1
    elif lst[mid] < x:
        start = mid + 1
    else:
        end = mid - 1

start_pos = res
print("start is :" + str(start_pos))

start = 0
end = len(lst) - 1
res = -1
# lst = [1, 2, 2, 2, 2, 2, 2, 3, 4, 5, 5, 5, 5, 67, 123, 125]
while start <= end:
    mid = start + (end - start) // 2
    if lst[mid] == x:
        res = mid
        start = mid + 1
    elif lst[mid] < x:
        start = mid + 1
    else:
        end = mid - 1

end_pos = res
print("end is :" + str(end_pos))

# Input: arr[] = [5, 6, 7, 8, 9, 10, 1, 2, 3], key = 3
# Output: 8
# Explanation: 3 is present at index 8 in arr[].
#
# Input: arr[] = [3, 5, 1, 2], key = 6
# Output: -1
# Explanation: 6 is not present in arr[].
#
# Input: arr[] = [33, 42, 72, 99], key = 42
# Output: 1
# Explanation: 42 is found at index 1.

# lst = [5, 6, 7, 8, 9, 10, 1, 2, 3]
# key = 3
#
# res = -1
# start = 0
# end = len(lst) - 1
#
# while start <= end:
#     mid = start + (end - start) // 2
#     if lst[mid] < lst[mid - 1] and lst[mid] < lst[mid + 1]:
#         start
#     else:
#         res = mid
#
# print(res)


# nums = [3, 1, 2]
# start = 0
# end = len(nums) - 1
# res = 100
# while start <= end:
#     mid = start + (end - start) // 2
#     if nums[start] <= nums[mid]:
#         print("res:" + str(res))
#         print("nums[start]:" + str(nums[start]))
#         if res > nums[start]:
#             res = nums[start]
#         start = mid + 1
#     else:
#         print("res:" + str(res))
#         print("nums[mid]:" + str(nums[mid]))
#         if res > nums[mid]:
#             res = nums[mid]
#             print("res:" + str(res))
#         end = mid - 1
# print(res)


"""
:type nums: List[int]
:rtype: int
"""
# nums = [1, 1, 2, 3, 3, 4, 4, 8, 8]
nums = [3, 3, 7, 7, 10, 11, 11]
start = 0
end = len(nums) - 1
if end == 0:
    res = nums[end]
if nums[start] != nums[start + 1]:
    res = nums[start]
if nums[end] != nums[end - 1]:
    res = nums[end]
start = start + 1
end = end - 1
while start <= end:
    mid = start + (end - start) // 2
    if nums[mid] != nums[mid - 1] and nums[mid] != nums[mid + 1]:
        res = nums[mid]
        break
    elif (mid % 2 != 0 and nums[mid] == nums[mid - 1]) or (mid % 2 == 0 and nums[mid] == nums[mid + 1]):
        start = mid + 1
    else:
        end = mid - 1
print(res)
