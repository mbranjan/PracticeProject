# SELECTION SORT:
# ===============
# Step 1: Find the minimum element in array, swap it with 1st position. [Array index considered from 0 to n-2]
# Step 2: Consider the starting index of aaray from position 1 to n-2.
#         Follow step 1 (find minimum and replace in start position)

# Sorting with finding the minimum and sorting from left end.
# ============================================================
# lst = [13, 46, 24, 52, 20, 9]
#
# for i in range(len(lst) - 1):
#     mini = i
#     j = i
#
#     while j < len(lst):
#         if lst[j] < lst[mini]:
#             mini = j
#         j = j + 1
#
#     temp = lst[mini]
#     lst[mini] = lst[i]
#     lst[i] = temp
#
# print(lst)

# Sorting with finding the maximum and sorting from right end.
# ============================================================
# lst = [13, 46, 24, 52, 20, 9]
#
# i = len(lst) - 1
# while i > 0:
#     j = i
#     maxi = i
#     while j >=0:
#         if lst[j] > lst[maxi]:
#             maxi = j
#         j = j - 1
#     temp = lst[maxi]
#     lst[maxi] = lst[i]
#     lst[i] = temp
#     i = i - 1
#
# print(lst)


