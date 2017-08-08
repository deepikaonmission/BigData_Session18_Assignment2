/* <<<<<<<<<<<------------------ QUERIES ------------------------>>>>>>>>>>>
1) Which route is generating the most revenue per year
2) What is the total amount spent by every user on air-travel per year
3) Considering age groups of < 20 , 20-35, 35 > ,Which age group is travelling the most every year.
*/

import org.apache.spark.sql.{Column, Row, SQLContext, SparkSession} //explanation is already given in Assignment18.1
object Session18Assign2 extends App{
  val spark = SparkSession.builder()
    .master("local")
    .appName("Session18Assign2")
    .config("spark.sql.warehouse.dir", "file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 18/Assignments/Assignment2/DataSet")
    .getOrCreate()
  //Explanation is already given in Assignment 18.1

  val df1 = spark.sqlContext.read.csv("file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 18/Assignments/Assignment2/DataSet/S18_Dataset_Holidays.txt")
  val df2 = spark.sqlContext.read.csv("file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 18/Assignments/Assignment2/DataSet/S18_Dataset_Transport.txt")
  val df3 = spark.sqlContext.read.csv("file:///G:/ACADGILD/course material/Hadoop/Sessions/Session 18/Assignments/Assignment2/DataSet/S18_Dataset_User_details.txt")

  /*Explanation of above three lines
  -> to work with spark sql, sqlContext is used
  -> here df1,df2,df3 variables with type sql.DataFrame are created by reading csv file from the specified locations
  */


  df1.show()  //to show the data inside DataFrames df1,df2,df3 in tabular format, show() method is used
  //REFER Screenshot 1 for output

  df2.show()
  //REFER Screenshot 2 for output

  df3.show()
  //REFER Screenshot 3 for output


  import spark.implicits._         //to work with case class this import is required just before case class
  case class holidaysClass(user_id:Int, src:String, dest:String, travel_mode:String, distance:Int, year_of_travel:Int)
  /* Explanation of above line
  -> case class "holidaysClass" is created, in order to
     * provide descriptive name to columns
     * and to infer schema (i.e. which are numeric columns, string columns etc.)
   */

  val df4 = df1.map{
    case Row (
    a:String,
    b:String,
    c:String,
    d:String,
    e:String,
    f:String) => holidaysClass(user_id=a.toInt,src=b,dest=c,travel_mode=d,distance=e.toInt,year_of_travel=f.toInt)
  }
  /* Explanation of above lines
  -> df4 -->> Dataset[Session18Assign2.holidaysClass] is created
  -> where df1 is mapped with case class
  */

  df4.show()
  //REFER Screenshot 4 for output

  case class transportClass(travel_mode:String, cost_per_unit:Int)

  val df5= df2.map{
    case Row(
    a:String,
    b:String
    ) => transportClass(travel_mode=a,cost_per_unit=b.toInt)
  }

  df5.show()
  //REFER Screenshot 5 for output

  case class userClass(user_id:Int,name:String,age:Int)

  val df6 = df3.map{
    case Row(
    a:String,
    b:String,
    c:String
    ) => userClass(user_id=a.toInt,name=b,age=c.toInt)
  }

  df6.show()
  //REFER Screenshot 6 for output

  df4.createOrReplaceTempView("holidaysTable")
  df5.createOrReplaceTempView("transportTable")
  df6.createOrReplaceTempView("userTable")

  /* Explanation of above three lines
  -> holidaysTable Temporary View is created from Dataset df4
  -> transportTable Temporary View is created from Dataset df5
  -> userTable Temporary View is created from Dataset df6
  */

  spark.sql("select * from holidaysTable").show()   //data is selected using select query from holidaysTable, and displayed in tabular format
  //REFER Screenshot 7 for output

  spark.sql("select * from transportTable").show()  //data is selected using select query from transportTable, and displayed in tabular format
  //REFER Screenshot 8 for output

  spark.sql("select * from userTable").show()  //data is selected using select query from userTable, and displayed in tabular format
  //REFER Screenshot 9 for output

 //<<<<<<<<<------------ QUERY 1 ----------->>>>>>>>>>
  //1. Which route is generating the most revenue per year?

  val revenuePerRoute = spark.sql("select h.year_of_travel as year,(h.src,h.dest) as route, sum(t.cost_per_unit) as revenue from holidaysTable h" +
    " join transportTable t" +
    " where h.travel_mode = t.travel_mode" +
    " group by h.year_of_travel,(h.src,h.dest)" +
    " order by h.year_of_travel,(h.src,h.dest)")     //here, h.src,h.dest can be written without (), will work same way

  /*Explanation
  -> above query finds "per year, per route i.e. (h.src,h.dest) the sum of cost of travel" from holidaysTable and transportTable
  -> by using join based on travel_mode condition i.e. h.travel_mode=t._travel_mode
  -> and by using group by clause on h.year_of_travel and (h.src and h.dest)
  -> result of outer query is sorted on the basis of year and route in ascending order (default ordering)
  -> finally revenuePerRoute DataFrame is generated
   */

  revenuePerRoute.show(30)          //shows the result in tabular format
  //REFER Screenshot 10 for output

  revenuePerRoute.createOrReplaceTempView("revenuePerRouteTable")    //creates temporary view from revenuePerRoute dataframe

  spark.sql("select year,route from revenuePerRouteTable" +
    " where (year,revenue) IN" +
    " (select year,max(revenue) from revenuePerRouteTable" +
    "  group by year)" +
    "  order by year").show()
  /*Explanation of above query
  -> year and route are fetched from revenuePerRouteTable
  -> where (year,revenue) match year,max(revenue) from subquery based on grouping on year field
  -> outer query is sorted on the basis of year in ascending order (default ordering)
  -> finally result is displayed in tabular format using show() method
  */
  //REFER Screenshot 11 for output of above query


  //<<<<<<<<<------------ QUERY 2 ----------->>>>>>>>>>
  //2. What is the total amount spent by every user on air-travel per year?

  spark.sql("select a.year_of_travel as year,a.user_id as user, sum(b.cost_per_unit) as totalAmount from" +
    " holidaysTable a join transportTable b" +
    " where a.travel_mode=b.travel_mode" +        //no need to mention a.travel_mode="airplane" because travel_mode in holidaysTable is airplane only
    " group by a.year_of_travel,a.user_id" +
    " order by a.year_of_travel,a.user_id").show()
  /*Explanation of above query
  -> here, per year_of_travel and per user_id, the sum of cost of travel is found from holidaysTable and transportTable
  -> based on join condition where travel_mode of holidays_Table matches travel_mode of transportTable i.e. "airplane" only
  -> then result is ordered on the basis of year wise first then users wise in ascending order (default ordering)
  -> finally, result is displayed in tabular format using show() method
   */
  //REFER Screenshot 12 for output of above query


  //<<<<<<<<<------------ QUERY 3 ----------->>>>>>>>>>
  //3. Considering age groups of < 20 , 20-35, 35 > ,Which age group is travelling the most every year

  /*val abc =df6.map(Row => {(Row.user_id,Row.name,
    if(Row.age<20) (Row.age,"ageGP1")
    else if(Row.age>=20 && Row.age<=35) (Row.age,"ageGP2")
    else (Row.age,"ageGP3"))
  })       //output (e.g.) -->> _1,_2,_3(_1,_2)*/       //not used  here, because nested tuple is created



  val df6_withAgeGP =df6.map(Row => {
    if(Row.age<20) (Row.user_id,Row.name,Row.age,"ageGP1 (< 20)")
    else if(Row.age>=20 && Row.age<=35) (Row.user_id,Row.name,Row.age,"ageGP2 (20-35)")
    else (Row.user_id,Row.name,Row.age,"ageGP3 (> 35)")
  })
  //output (e.g.) -->> _1,_2,_3,_4

  /*Explanation
  -> here, ageGroups are created i.e.
     * ageGP1 for age < 20
     * ageGP2 for age between 20-35
     * ageGP3 for age > 35
  -> using, Row where it represents complete rowset
     * df6 is mapped using map
     * where Row is received and Row.user_id,Row.name and Row.age based on different conditions are returned
  -> finally, a new Dataset df6_withAgeGP is created with fields _1,_2,_3,_4
   */

  //df6_withAgeGP.printSchema()      //prints schema
  df6_withAgeGP.show()             //shows result in tabular format
  //REFER Screenshot 13 for output

  df6_withAgeGP.createOrReplaceTempView("userAgeGPTable")
  //to rename fields of above dataset this temporary view is created

  val df6_withAgeGPAlias =spark.sql("select _1 as user_id,_2 as name,_3 as age,_4 as age_group from userAgeGPTable")
  //query provides alias to all fields of above view

  //df6_withAgeGPAlias.printSchema()      //prints schema
  df6_withAgeGPAlias.show()               //shows result in tabular format
  //REFER Screenshot 14 for output


  df6_withAgeGPAlias.createOrReplaceTempView("userAgeGPTableNew")      //temporary view is created

  //spark.sql("select * from userAgeGPTableNew").show()        //result is shown in tabular format

  val countOfusersAgeWise = spark.sql("select h.year_of_travel as year,u.age_group,count(h.user_id) as countOfUsers from userAgeGPTableNew u " +
    " join holidaysTable h" +
    " where u.user_id = h.user_id" +
    " group by h.year_of_travel,u.age_group" +
    " order by h.year_of_travel,u.age_group")

  /*Explanation of above query
  -> here, per year, per age_group the count of user_id is found from userAgeGPTableNew and holidaysTable using
  -> join where user_id of userAgeGPTableNew matches with user_id of holidaysTable
  -> result is ordered on the basis of year and age_group in ascending order (default ordering)
  -> finally DataFrame countOfusersAgeWise is created
   */

  countOfusersAgeWise.show()           //result is shown in tabular format
  //REFER Screenshot 15 for output

  countOfusersAgeWise.createOrReplaceTempView("countOfUsersAgeWiseTable")    //temporary view is created

  //spark.sql("select * from countOfUsersAgeWiseTable")

  spark.sql("select year,age_group from countOfUsersAgeWiseTable" +
    " where (year,countOfUsers) IN " +
    " (select year,max(countOfUsers) from countOfUsersAgeWiseTable" +
    " group by year" +
    " order by year)").show()
  /*Explanation
  -> in above query, age_group from countOfUsersAgeWiseTable is fetched where
  -> (year,countOfUsers) receive rows from subquery i.e.
     "select year,max(countOfUsers) from countOfUsersAgeWiseTable group by year order by year"
     in subquery, year and max(countOfUsers) is fetched based on group year
     and result is sorted in ascending order
  -> finally result is shown in tabular format
   */
  //REFER Screenshot 16 for output

}













//Practice work for query 3
/*val ageGP1_Users = spark.sql("select h.year_of_travel as year,count(h.user_id) as countOfUsers_AgeGP1" +
    " from userTable u" +
    " join holidaysTable h" +
    " where u.user_id = h.user_id" +
    " and u.age<20" +
    " group by h.year_of_travel")


  ageGP1_Users.show()

  val ageGP2_Users = spark.sql("select h.year_of_travel as year,count(h.user_id) as countOfUsers_AgeGP2" +
    " from userTable u" +
    " join holidaysTable h" +
    " where u.user_id = h.user_id" +
    " and u.age between 20 and 35" +
    " group by h.year_of_travel")
  ageGP2_Users.show()

  val ageGP3_Users = spark.sql("select h.year_of_travel as year,count(h.user_id) as countOfUsers_AgeGP3" +
    " from userTable u" +
    " join holidaysTable h" +
    " where u.user_id = h.user_id" +
    " and u.age>35" +
    " group by h.year_of_travel")
  ageGP3_Users.show()*/

