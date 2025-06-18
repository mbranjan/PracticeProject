# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press ⌘F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # print_hi('PyCharm')
    # print("Enter the first number")

    choice = 'y'
    while choice == 'y':
        operator = input(""" Select one of the operations as below:
         '+' as Addition or 
         '-' as Subtraction or 
         '*' as Multiplication or 
         '/' as Division \n""")
        first_num = input("Enter the first number: ")
        # print("Enter the second number")
        second_num = input("Enter the second number: ")

        if operator == '+':
            if (int(first_num) == 56 or int(first_num) == 9) and (int(second_num) == 56 or int(second_num) == 9):
                print("Sum of your entered numbers is: " + str(77))
            else:
                print("Sum of your entered numbers is: " + str(int(first_num) + int(second_num)))

        elif operator == '-':
            if (int(first_num) == 56) and (int(second_num) == 9):
                print("Subtraction of your entered numbers is: " + str(37))
            else:
                print("Subtraction of your entered numbers is: " + str(int(first_num) - int(second_num)))

        elif operator == '*':
            if (int(first_num) == 45 or int(first_num) == 3) and (int(second_num) == 45 or int(second_num) == 3):
                print("Multiplication of your entered numbers is: " + str(555))
            else:
                print("Multiplication of your entered numbers is: " + str(int(first_num) * int(second_num)))

        else:
            if (int(first_num) == 56) and (int(second_num) == 6):
                print("Division of your entered numbers is: " + str(4))
            else:
                division = round(int(first_num) / int(second_num))
                print("Division of your entered numbers is: " + str(division))

        choice = input("""\n \n Do you want to do another calculation. Please enter 'y' for Yes and 'n' for No \n """)
