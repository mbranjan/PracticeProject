# QUESTION - 1
#####################################################################################################################
""" Write a special cipher that is a combination of Caesar’s cipher followed by a
simple RLE. The function should receive a string and the rotation number as
parameters. Input: special Cipher(“AABCCC”, 3) Output: D2EF3 """


def special_cipher(txt, rotation):
    # txt = "AABCCC"
    # rotation = 3
    temp = ""
    for ch in txt:
        if ch.isalpha():
            if ch.isupper():
                base = ord("A")
            else:
                base = ord("a")
            char = chr((ord(ch) + rotation - base) % 26 + base)
            temp = temp + char
        else:
            temp = temp + ch

    dct = {}
    for ch in temp:
        if ch in dct:
            dct[ch] += 1
        else:
            dct[ch] = 1
    ans = ""
    for key in dct:
        if dct[key] > 1:
            ans = ans + key
            ans = ans + str(dct[key])
        else:
            ans = ans + key
    print(ans)


if __name__ == "__main__":
    special_cipher("AABCCC", 3)

# QUESTION -2
#####################################################################################################################
""" Given a linked list  [1,2,3,4,5,6,7,8]. It should be reversed by window of 3 elements. 
 output should be [3, 2, 1, 6, 5, 4, 8, 7]
"""
# lst = [1,2,3,4,5,6,7,8]
# w = 3
#
# i = 0
# j = i + w - 1
# k = 3
# temp = []
# ans =[]
# cnt = 0
#
# while cnt < len(lst):
#   k = j - i + 1
#   n = j
#   while k > 0:
#     temp.append(lst[n])
#     k = k - 1
#     n = n - 1
#     cnt = cnt + 1
#   ans = ans + temp
#   temp =[]
#   i = j + 1
#   j = min(len(lst)-1,i + w - 1)
# print(ans)
