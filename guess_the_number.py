num_of_guess = 1
max_num_of_guess = 5
number = 15

inp_num = int(input("Guess the number: "))
if inp_num == number:
    print("Congratulations! You guessed the correct number in first attempt.")
else:
    while inp_num != number:
        if num_of_guess < max_num_of_guess:
            if inp_num > number:
                print("OOPS.. Try Again!! Guess a smaller Number. You are left with {} more guesses.".format(
                    max_num_of_guess - num_of_guess))
            else:
                print("OOPS.. Try Again!! Guess a bigger Number. You are left with {} more guesses.".format(
                    max_num_of_guess - num_of_guess))
            num_of_guess += 1
            inp_num = int(input("Guess the number: "))

        else:
            print("Sorry!! Gave Over.. You have exceeded number of guesses.")
            break
    if inp_num == number:
        print("Congratulations! You guessed the correct number in {} attempts.".format(num_of_guess))
