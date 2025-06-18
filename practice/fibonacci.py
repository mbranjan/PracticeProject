input_num = int(input("Please enter the size of the series: "))
print(type(input_num))
n = 0
x = 0
y = 1
arr = [x,y]

while n < input_num - 2:
    z = x + y
    arr.append(z)
    x = y
    y = z
    n = n+1

print(arr)
