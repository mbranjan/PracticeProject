while (True):
    print('')
    print('****************************************')
    print('* WELCOME TO LIBRARY MANAGEMENT SYSTEM *')
    print('****************************************')
    print('')
    print('         1.ADD NEW BOOK TO LIBRARY')
    print('         2.SEARCH A BOOK')
    print('         3.FIND ALL BOOKS')
    print('         4.UPDATE BOOK INFORMATION')
    print('         5.REMOVE A BOOK FROM LIBRARY')
    print('         6.GRAPH')
    print('         7.EXIT')
    print('')
    print('****************************************')
    ch = int(input('ENTER YOUR CHOICE: '))
    if (ch == 1):
        import pymysql

        conn = pymysql.connect(host='localhost', user='root', password='rootuser', database='LIBRARY')
        a = conn.cursor()
        book_id = int(input('Enter Book ID :'))
        book_name = input('Enter Book Name :')
        author_name = input('Enter Author Name :')
        rack_number = input('Enter Rack Number [Like... R1/R2/R3/.../R10] :')
        book_price = int(input('Enter Book Price :'))
        insert_query = 'INSERT INTO BOOKS VALUES(' + str(
            book_id) + ',"' + book_name + '","' + author_name + '","' + rack_number + '",' + str(book_price) + ')'
        a.execute(insert_query)
        print('You have successfully added the book "' + book_name + '" to library.')
        conn.commit()

    elif (ch == 2):
        import pymysql

        conn = pymysql.connect(host='localhost', user='root', password='rootuser', database='LIBRARY')
        a = conn.cursor()
        search_book_name = input('Enter a Book Name to search: ')
        search_query = "SELECT * FROM BOOKS WHERE BOOK_NAME like('%" + search_book_name + "%')"
        a.execute(search_query)
        data = a.fetchall()
        if (len(data) == 0):
            print('Entered Book - ', search_book_name, ' does not exist')
        else:
            for i in data:
                print("###########################################")
                print('Book ID: ', str(i[0]))
                print('Book Name: ', i[1])
                print('Author Name: ', i[2])
                print('Rack Number: ', i[3])
                print('Book Price: ', str(i[4]))
            print("###########################################")
            print("Search Ends Here.")
        conn.commit()

    elif (ch == 3):
        import pymysql

        conn = pymysql.connect(host='localhost', user='root', password='rootuser', database='LIBRARY')
        a = conn.cursor()
        search_query_all = 'SELECT * FROM BOOKS'
        a.execute(search_query_all)
        data = a.fetchall()
        for i in data:
            for j in i:
                print(j, end=' ||')
            print()
        conn.commit()

    elif (ch == 4):
        import pymysql

        conn = pymysql.connect(host='localhost', user='root', password='rootuser', database='LIBRARY')
        a = conn.cursor()
        update_book_id = int(input('Enter the Book ID to be updated: '))
        sel_query = 'SELECT * FROM BOOKS WHERE BOOK_ID=' + str(update_book_id)
        a.execute(sel_query)
        data = a.fetchall()
        if (len(data) == 0):
            print('Entered Book ID - ', update_book_id, 'does not exist')
        else:
            print('Exiting details:')
            print('Book ID: ', update_book_id)
            print('Book Name: ', data[0][1])
            print('Author Name: ', data[0][2])
            print('Rack Number: ', data[0][3])
            print('Book Price: ', data[0][4])

            new_book_name = input('Enter Book Name: ')
            new_author_name = input('Enter Author Name: ')
            new_rack_number = input('Enter Rack Number [Like... R1/R2/R3/.../R10]: ')
            new_book_price = int(input('Enter Book Price: '))

            update_query = 'UPDATE BOOKS SET BOOK_NAME="' + new_book_name + '", AUTHOR="' + new_author_name + '", RACK_NUMBER="' + new_rack_number + '", BOOK_PRICE=' + str(
                new_book_price) + ' WHERE BOOK_ID=' + str(update_book_id)
            # print(update_query)
            a.execute(update_query)
            print('Book ID - ', str(update_book_id), ' has been updated successfully:')
        conn.commit()

    elif (ch == 5):
        import pymysql

        conn = pymysql.connect(host='localhost', user='root', password='rootuser', database='LIBRARY')
        a = conn.cursor()
        delete_book_id = int(input('Enter the Book_ID to delete:'))
        sel_query = 'SELECT BOOK_NAME FROM BOOKS WHERE BOOK_ID=' + str(delete_book_id)
        a.execute(sel_query)
        data = a.fetchall()

        if (len(data) == 0):
            print('Entered Book_Id -', str(delete_book_id), 'does not exist')
        else:
            for i in data:
                for j in i:
                    delete_book_name = j
            delete_query = 'DELETE FROM BOOKS WHERE BOOK_ID=' + str(delete_book_id)
            a.execute(delete_query)
            print('Book "' + delete_book_name + '" has been deleted successfully from Library.')
        conn.commit()

    elif (ch == 6):
        import pymysql
        import matplotlib.pyplot as plt

        conn = pymysql.connect(host='localhost', user='root', password='rootuser', database='LIBRARY')
        a = conn.cursor()
        s = 'SELECT BOOK_NAME , BOOK_PRICE FROM BOOKS'
        a.execute(s)
        data = a.fetchall()
        L1 = []
        L2 = []
        for i in data:
            L1.append(i[0])
            L2.append(int(i[1]))

        plt.bar(L1, L2)
        plt.xlabel('Book name')
        plt.ylabel('Book Price')
        plt.title('Book Price Chart')

        plt.show()
        conn.commit()
    elif (ch == 7):
        break
    else:
        print('Invalid choice...please enter choice in between 1 to 7')
