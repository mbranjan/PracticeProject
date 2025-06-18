########################################################################################################
# Finding minimum in rotated array.
########################################################################################################
# lst = [19, 23, 2, 4, 5, 7, 9, 12, 15]
# lst = [5, 7, 9, 12, 15, 19, 23, 2, 4]

import sys
# lst = [19, 23, 2, 4, 5, 7, 9, 12, 15]
lst = [2,4, 5, 7, 9, 12, 15, 19, 23]

start = 0
end = len(lst) - 1
ans = sys.maxsize

while start <= end:
    mid = (start + end) // 2
    if lst[start] <= lst[end]:
        if ans > lst[start]:
            ans = lst[start]
        break
    if lst[start] <= lst[mid]:
        if ans > lst[start]:
            ans = lst[start]
        start = mid + 1
    else:
        if ans >= lst[mid]:
            ans = lst[mid]
        end = mid - 1

print(ans)


############################################################################################################
# Find the number of times a sorted array is rotated. Almost equivalent to finding minimum in rotated array.
############################################################################################################
import sys
# lst = [19, 23, 4, 5, 7, 9, 12, 15,16]
lst = [5, 7, 9, 12, 15, 19, 23, 2, 4]

start = 0
end = len(lst) - 1
ans = sys.maxsize
pos = -1
if lst[start] <= lst[end]:
    print("List is already sorted")

while start <= end:
    mid = (start + end) // 2
    if lst[start] <= lst[end]:
        if ans > lst[start]:
            ans = lst[start]
            pos = start
        break
    elif lst[start] <= lst[mid]:
        if ans > lst[start]:
            ans = lst[start]
            pos = start
        start = mid + 1
    else:
        if ans >= lst[mid]:
            ans = lst[mid]
            pos = mid
        end = mid - 1

print(ans, pos)





