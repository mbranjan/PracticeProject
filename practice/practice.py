# def countOfAnagram(txt, pat):
#     i = 0
#     j = 0
#     dct = {}
#     for chr in pat:
#         if chr in dct:
#             dct[chr] = dct[chr] + 1
#         else:
#             dct[chr] = 1
#     cnt = len(dct)
#     size = len(dct)
#     k = len(pat)
#
#     ans = 0
#     # print(dct)
#     while j < len(txt):
#         print(i, j, cnt)
#         print(dct)
#         if txt[j] in dct:
#             dct[txt[j]] -= 1
#             if dct[txt[j]] == 0:
#                 cnt -= 1
#         print(dct)
#
#         if j - i + 1 < k:
#             j = j + 1
#         elif j - i + 1 == k:
#             print("cnt:" + str(cnt),"size:"+ str(size))
#             if cnt == 0:
#                 ans = ans + 1
#                 print("ans:" + str(ans))
#             if txt[i] in dct:
#                 dct[txt[i]] += 1
#                 if dct[txt[i]] == 1:
#                     print("txt[i]:" + txt[i])
#                     cnt = cnt + 1
#             i = i + 1
#             j = j + 1
#     return ans
#
#
# if __name__ == '__main__':
#     # print(countOfAnagram("forxxorfxdofr", "for"))
#     print(countOfAnagram("aabaabaa", "aaba"))
#
#
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




lst = [4, 6, 10, 12, 18, 20, 25]
k = 18

start = 0
end = len(lst) - 1
res = -1

while start <= end:
    mid = (end + start) // 2
    if lst[mid] == k:
        res = mid
        break
    elif lst[mid] < k:
        start = mid + 1
    else:
        end = mid - 1

print(res)








