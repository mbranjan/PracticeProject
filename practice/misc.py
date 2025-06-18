# Input: lst = [1, 2, 3, 4, 5, 6, 7, 8]
# Output: [3, 2, 1, 6, 5, 4, 8, 7]

lst = [1, 2, 3, 4, 5, 6, 7, 8]
w = 3

i = 0
j = i + w - 1
k = 3
temp = []
ans = []
cnt = 0

while cnt < len(lst):
    k = j - i + 1
    n = j
    while k > 0:
        temp.append(lst[n])
        k = k - 1
        n = n - 1
        cnt = cnt + 1
    ans = ans + temp
    temp = []
    i = j + 1
    j = min(len(lst) - 1, i + w - 1)
print(ans)
