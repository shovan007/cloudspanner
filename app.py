import json
import random as r
from google.cloud import spanner

def generate_uuid():
    '''This will generate alphanumeric unique ID '''
    random_string = ''
    random_str_seq = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    uuid_format = [5]
    for n in uuid_format:
        for i in range(0,n):
            random_string += str(random_str_seq[r.randint(0, len(random_str_seq) - 1)])
    return random_string


def user_input():
    '''Method to accpet file name as input to enter the User data'''
    try:
        f_name = input("Enter File name to upload User Detail and Feedback Data      ")
        with open(f_name, 'r') as json_file:
            line = json_file.readline()
            count = 1
            def read_then_write(transaction):
              results = transaction.execute_sql("""SELECT Id FROM UserDetails WHERE phonenumber='%s'""" % (pno))
              ud_id = False
              for result in results:
                ud_id = result[0]
              if ud_id:
                user_update(input_user, transaction)
                user_feedback(input_user, ud_id, transaction=transaction)
              else:
                ud_id = generate_uuid()
                fb_id = generate_uuid()
                user_details(input_user, ud_id, transaction=transaction)
                user_feedback(input_user, ud_id, fb_id, transaction=transaction)

            while line:
                input_user = json.loads(line)
                pno = str(input_user["pno"])
                database.run_in_transaction(read_then_write)
                print(str(count) + " Record Successfully Inserted/Updated")
                count = count + 1
                line = json_file.readline()

    except Exception as e:
        print(e)



def user_update(input_json,transaction):
    '''Method to update the user data if user already exists'''
    try:
        pno = str(input_json["pno"])
        us_name = str(input_json["name"])
        us_eid = str(input_json["emailid"])
        us_selfdob = str(input_json["selfdob"])
        us_spdob = str(input_json["spousedob"])
        us_ma = str(input_json["anniversary"])
        row_ct = transaction.execute_update("""UPDATE UserDetails SET Name = '%s' , Emailid='%s', birthday='%s', SpouseBirthday='%s', Anniversary='%s' where phonenumber='%s'""" % (
                    us_name, us_eid, us_selfdob, us_spdob, us_ma, pno,))
        print("User Record Updated successfully ")
    except Exception as e:
        print(e)


def user_details(input_json=None, ud_id=None,transaction=None):
    '''Method to insert the user data into cloud spanner'''
    try:
        user_id = ud_id
        ud_rname = str(input_json["name"])
        ud_pn = str(input_json["pno"])
        ud_eid = str(input_json["emailid"])
        ud_selfdob = str(input_json["selfdob"])
        ud_spdob = str(input_json["spousedob"])
        ud_ma = str(input_json["anniversary"])
        row_ct = transaction.execute_update("""INSERT UserDetails (Id,Name,phonenumber,Emailid,birthday,Spousebirthday,Anniversary) VALUES ("%s", "%s", "%s", "%s", "%s","%s","%s")""" % (
            user_id, ud_rname, ud_pn, ud_eid, ud_selfdob, ud_spdob, ud_ma))
        print("User Detail Record Insertion Successful")

    except Exception as e:
        print(e)


def user_feedback(input_json=None, us_id=None,fb_id=None,transaction=None):
    '''Method to insert the user feedback into cloud spanner'''
    try:
        fb_dov = str(input_json["dateofvisit"])
        fb_resid = str(input_json["restid"])
        fb_fq = int(input_json["foodquality"])
        fb_sq = int(input_json["servicequality"])
        fb_amb = int(input_json["ambience"])
        fb_music = int(input_json["music"])
        fb_vfm = int(input_json["valueformoney"])
        fb_clean = int(input_json["cleanliness"])
        fb_fv = int(input_json["foodvariety"])

        row_ct = transaction.execute_update("""INSERT userfeedback (feedbackid,UserId,VisitDate, id,FoodQuality,ServiceQuality,Ambience,LiveMusic,ValueForMoney,Cleanliness,FoodVariety) VALUES
         ("%s","%s", "%s", "%s", %s, %s,%s,%s,%s,%s,%s)""" % (fb_id,us_id, fb_dov, fb_resid, fb_fq, fb_sq, fb_amb, fb_music, fb_vfm, fb_clean, fb_fv))

        print("Feedback Record Insertion Successful")

    except Exception as e:
        print(e)



def register_restaurant():
    '''Method to accpet file name as input for populating restaurants data'''
    try:
        f_name = input("Enter File name to upload Restaurant Data      ")
        with open(f_name, 'r') as json_file:
            line = json_file.readline()
            count = 1
            while line:
                input_json1 = json.loads(line)
                uid = generate_uuid()
                r_rname = str(input_json1["name"])
                r_cuisine = str(input_json1["cuisine"])
                r_region = str(input_json1["region"])
                r_location = str(input_json1["location"])
                def insert_restaurant(transaction):
                    row_ct = transaction.execute_update("""INSERT Restaurant (Id,Name,Cuisine,Region,Location) VALUES ("%s", "%s", "%s", "%s", "%s") """ % (
                uid, r_rname, r_cuisine, r_region, r_location))
                database.run_in_transaction(insert_restaurant)
                print("Record No - " + str(count) + "  Insertion Successful for Restaurant name -- " + r_rname)
                count = count + 1
                line = json_file.readline()

    except Exception as e:
        print(e)


def delete_restaurant():
    '''Method to accpet file name as input to delete restaurant data'''
    try:
        f_name = input("Enter File name to Delete Restaurant Data      ")
        with open(f_name, 'r') as json_file:
            line = json_file.readline()
            while line:
                input_json1 = json.loads(line)
                restid = str(input_json1["id"])
                data = (restid,)
                def delete_restaurant(transaction):
                    row_ct = transaction.execute_update("""DELETE Restaurant WHERE Id = '%s'"""%(data))
                database.run_in_transaction(delete_restaurant)
                print(" Restaurant Successfully Deleted")
                line = json_file.readline()
    except Exception as e:
        print(e)


"""QUERIES"""
def query_1():
    try:
        def query1(transaction):
          results = transaction.execute_sql("select r1.name from (select id, avg(((foodquality+servicequality+ambience+livemusic+valueformoney+cleanliness+foodvariety)*1.0)/7) avgratingacrossall from userfeedback group by id order by 1 desc limit 1)tbla, Restaurant r1 where tbla.id=r1.id")
          for result in results:
            print(result,"\n")
        database.run_in_transaction(query1)
    except Exception as e:
        print(e)


def query_2():
  try:
    print("Enter parameter on which restaurants has to be compared  :")
    parameter = input()
    if parameter == 'foodquality':
      def query2(transaction):
        results = transaction.execute_sql(
          "select r1.name from (select id, avg(foodquality) avgratingselected from userfeedback group by id order by 1 desc limit 2)tbla, Restaurant r1 where tbla.id=r1.id")
        for result in results:
          print(result, "\n")
      database.run_in_transaction(query2)

    if parameter == 'servicequality':
      def query2(transaction):
        results = transaction.execute_sql(
          "select r1.name from (select Id, avg(servicequality) avgratingselected from userfeedback group by id order by 1 desc limit 2)tbla, Restaurant r1 where tbla.id=r1.Id")

        for result in results:
          print(result, "\n")

      database.run_in_transaction(query2)
  except Exception as e:
    print(e)


def query_3():
  try:
    print("Enter the date for which the birthday has to be checked :")
    date_input = input()

    def query3(transaction):
      results = transaction.execute_sql(
        """select Name, phonenumber, Emailid from UserDetails where EXTRACT(MONTH FROM birthday) =EXTRACT(MONTH FROM DATE '%s') and EXTRACT(DAY FROM birthday) between EXTRACT(DAY FROM DATE '%s') and EXTRACT(DAY FROM DATE '%s')+7""" % (
      date_input, date_input, date_input))

      for result in results:
        print(result, "\n")

    database.run_in_transaction(query3)
  except Exception as e:
    print(e)


def query_4():
  try:
    print("Enter the date for which occassion has to checked :")
    date_input = input()
    def query4(transaction):
      results = transaction.execute_sql(
        """select Name, PhoneNumber, emailId from UserDetails where EXTRACT(MONTH FROM birthday) =EXTRACT(MONTH FROM DATE '%s') or EXTRACT(MONTH FROM SpouseBirthday) =EXTRACT(MONTH FROM DATE '%s') and EXTRACT(MONTH FROM Anniversary) =EXTRACT(MONTH FROM DATE '%s')""" % (
      date_input, date_input, date_input))

      for result in results:
        print(result, "\n")

    database.run_in_transaction(query4)
  except Exception as e:
    print(e)


if __name__ == "__main__":
    try:
        spanner_client = spanner.Client()		
        instance = spanner_client.instance("democloudspanner")	
        database = instance.database("eatout")

        while True:
            print("Select the operations to perform:")
            print("1. Register Restaurant")
            print("2. Load User Feedback")
            print("3. Fetch the top rated restaurant")
            print("4. Top 2 basis on my input")
            print("5. List users with birthdays in next 7 days from the date specified")
            print("6. List users with any of there occasion in given month")
            print("7. Delete Restaurant")
            print("0. Exit")
            operation = input()
            if(operation=='1' or operation == 1):
                print("Selected: Register Restaurant")
                register_restaurant()
            if(operation == '2' or operation == 2):
                print("Selected: Load User Feedback")
                user_input()
            if(operation == '3' or operation== 3):
                print("Selected: Fetch top rated restaurant")
                query_1()
            if (operation == '4' or operation == 4):
                print("Selected: Top 2 on the basis of input")
                query_2()
            if (operation == '5' or operation == 5):
                print("Selected: List users with birthday")
                query_3()
            if (operation == '6' or operation == 6):
                print("Selected: List users with any occassion")
                query_4()
            if (operation == '7' or operation == 7):
                print("Selected: Delete Restaurant")
                delete_restaurant()
            if( operation == '0' or operation == 0):
                print("Thank You")
                break

    except Exception as e:
        print(e)
