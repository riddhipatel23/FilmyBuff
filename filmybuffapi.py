from flask import Flask, jsonify, request
import json
import requests
import re
import base64
import pymysql
import boto3
from config import connection_properties,awskey


app = Flask(__name__)

connection=pymysql.connect(**connection_properties)#diff

#Screen 1 API to for login
@app.route('/login',methods=['POST'])
def login():
    content=request.get_json()  
    screenname=content["ScreenName"]
    fbid=content["FacebookId"]
    userid=content["UserId"]
    password=content["Password"]
    userid=str(userid)
    
    try:
        cur=connection.cursor()
        """
        try:
            query="INSERT INTO usert(sreen_name,country) VALUES (\'"+screenname+"\',\'"+country+"\');"
            cur.execute(query)
            
        except:
            return "Screenname already exist!"
        cur.execute("commit")
        """
        if len(userid)==0 and len(fbid)==0:
            cur.execute("SELECT u_password from usert where screen_name=\""+screenname+"\";")
            pass_result=cur.fetchone()
            row_headers=[x[0] for x in cur.description]
            pass_result=dict(zip(row_headers,pass_result))
            password_check=pass_result['u_password']
            cur.execute("SELECT usert.user_id as \"Id\", usert.img_url as \"ImgUrl\",achievements.u_coins as \"Coins\",achievements.u_crowns as \"Crowns\",achievements.gold as \"Gold\",achievements.silver as \"Silver\",achievements.bronze as \"Bronze\",usert.wtg as \"WhereToGo\" FROM moviedb.usert INNER JOIN moviedb.achievements ON usert.user_id=achievements.user_id and usert.screen_name=\""+screenname+"\";")
        elif len(screenname)==0 and len(fbid)==0:
            cur.execute("SELECT u_password from usert where user_id=\""+userid+"\";")
            pass_result=cur.fetchone()
            row_headers=[x[0] for x in cur.description]
            pass_result=dict(zip(row_headers,pass_result))
            password_check=pass_result['u_password']
            cur.execute("SELECT usert.user_id as \"Id\", usert.img_url as \"ImgUrl\",achievements.u_coins as \"Coins\",achievements.u_crowns as \"Crowns\",achievements.gold as \"Gold\",achievements.silver as \"Silver\",achievements.bronze as \"Bronze\",usert.wtg as \"WhereToGo\" FROM usert INNER JOIN achievements ON usert.user_id=achievements.user_id and usert.user_id=\""+userid+"\";")
        elif len(userid)==0 and len(screenname)==0:
            cur.execute("SELECT usert.user_id as \"Id\", usert.img_url as \"ImgUrl\",achievements.u_coins as \"Coins\",achievements.u_crowns as \"Crowns\",achievements.gold as \"Gold\",achievements.silver as \"Silver\",achievements.bronze as \"Bronze\",usert.wtg as \"WhereToGo\" FROM usert INNER JOIN achievements ON usert.user_id=achievements.user_id and usert.fb_id=\""+fbid+"\";")
        if password==password_check:
            row_headers=[x[0] for x in cur.description] #this will extract row headers
            result=cur.fetchall()
            json_data=[]
            for r in result:
                json_data.append(dict(zip(row_headers,r)))
            response={'IsSuccess':True,'error':'null','Data':json_data}
        else:
            response={'IsSuccess':True,'error':'null','Data':[]}
        return jsonify(response)
    except Exception as e:
        response={'IsSuccess':False,'error':str(e)}
        return jsonify(response)

#Screen 1 API to get screenname
@app.route('/register/<screenname>',methods=['GET'])
def screennameAPI(screenname):
    try:
        cur=connection.cursor()
        cur.execute("select exists(select screen_name from usert where screen_name=\""+screenname+"\");")
        result=cur.fetchone()
        result=int(str(result[0]))
        e_result={
                'Available':''
        }
        if result:
            e_result['Available']=False 
        else:
            e_result['Available']=True
        response={'IsSuccess':True,'error':'null','Data':[]}
        response['Data']=e_result
        return jsonify(response)
    except Exception as e:
        response={'IsSuccess':False,'error':str(e)}
        return jsonify(response)

#Screen 2 API avatar retrieval 
@app.route('/avatar',methods=['GET'])
def avatarAPI():
    try:
        cur=connection.cursor()
        cur.execute("select * from avatar")
        row_headers=[x[0] for x in cur.description] #this will extract row headers
        result=cur.fetchall()
        json_data=[]
        response={'IsSuccess':True,'error':'null','Data':[]}
        for result in result:
            json_data.append(dict(zip(row_headers,result)))
        response['Data']=json_data
        return jsonify(response)
    except Exception as e:
        response={'IsSuccess':False,'error':str(e)}
        return jsonify(response)


#Screen 3 API user profile
@app.route('/profile',methods=['POST'])
def ProfileAPI():
    connection=pymysql.connect(**connection_properties)
    cur=connection.cursor()
    content=request.get_json()
    country=content["Country"]
    fbid=content["FB Id"]
    screenname=content["ScreenName"]
    fname=content["FirstName"]
    lname=content["LastName"]
    mstatus=content["MaritalStatus"]
    if mstatus=="True" or mstatus=="true":
        mstatus=1
    elif mstatus=="false" or mstatus=="False":
        mstatus=0
    prof=content["Profession"]
    email=content["Email Id"]
    aid=content["AvatarId"]
    abase64=content["AvatarBase64"]
    afb=content["AvatarFacebook"]
    sgenre=content["SelectedGenre"]
    sregion=content["SelectedRegion"]
    password=content["Password"]
    try:
        if abase64:
            with open("/tmp/output"+screenname+".jpg", "wb") as fh:
                fh.write(base64.b64decode(abase64))
            s3_client=boto3.client('s3',**awskey)
            s3_client.upload_file("/tmp/output"+screenname+".jpg","movie.buff.user.images",screenname+".jpg", ExtraArgs={'ACL': 'public-read','ContentType':'image/jpeg'})
            urls="https://s3.ap-south-1.amazonaws.com/movie.buff.user.images/"+screenname+".jpg"
            cur.execute("insert into usert(first_name,last_name,screen_name,u_password,marital_s,prof,gmail_id,img_url,favgenre,region) values (\""+fname+"\",\""+lname+"\",\""+screenname+"\",\""+str(password)+"\",\""+str(mstatus)+"\",\""+prof+"\",\""+email+"\",\""+urls+"\",\""+str(sgenre)+"\",\""+str(sregion)+"\");")
            cur.execute("commit")
        elif afb:
            cur.execute("insert into usert(first_name,last_name,fb_id,screen_name,u_password,marital_s,prof,gmail_id,img_url,favgenre,region) values (\""+fname+"\",\""+lname+"\",\""+fbid+"\",\""+screenname+"\",\""+str(password)+"\",\""+str(mstatus)+"\",\""+prof+"\",\""+email+"\",\""+afb+"\",\""+str(sgenre)+"\",\""+str(sregion)+"\");")
            cur.execute("commit")
        else:
            cur.execute("insert into usert(first_name,last_name,screen_name,u_password,marital_s,prof,gmail_id,avatar_id,favgenre,region) values (\""+fname+"\",\""+lname+"\",\""+screenname+"\",\""+str(password)+"\",\""+str(mstatus)+"\",\""+prof+"\",\""+email+"\",\""+str(aid)+"\",\""+str(sgenre)+"\",\""+str(sregion)+"\");")
            cur.execute("update usert set img_url=(select a_img_url from avatar where avatar_id="+str(aid)+") where usert.screen_name=\""+screenname+"\";")
            cur.execute("commit")
        cur.execute("SELECT user_id from usert WHERE screen_name=\""+screenname+"\";")
        userID=cur.fetchone()
        row_headers=[x[0] for x in cur.description]
        userID=dict(zip(row_headers,userID))
        userid=userID['user_id']
        cur.execute("insert into achievements(user_id) values ("+str(userid)+");")
        cur.execute("COMMIT;")
        cur.execute("Select usert.user_id as \"Id\", achievements.u_coins as \"Coins\", achievements.u_crowns as \"Crowns\" ,achievements.gold as \"Gold\",achievements.silver as \"Silver\",achievements.bronze as \"Bronze\",usert.img_url as \"ImgUrl\",usert.wtg as \"WhereToGo\" from achievements join usert on usert.user_id=achievements.user_id where usert.screen_name=\""+screenname+"\";")
        row_headers=[x[0] for x in cur.description] #this will extract row headers
        myresult=cur.fetchall()
        response={'IsSuccess':True,'error':'null','Data':[]}
        json_data=[]
        for result in myresult:
            json_data.append(dict(zip(row_headers,result)))
        response['Data']=json_data
        return jsonify(response)
    except Exception as e:
        response={'IsSuccess':False,'error':str(e)}
        return jsonify(response)    

#Screen 8 & 9 Main game API
@app.route('/getQuestions',methods=['POST'])
def startGameAPI():
    cur=connection.cursor()
    #read json
    content=request.get_json()
    isr=content["isRandom"]
    region=content["Region"]
    noq=content["noQ"]
    userid=content["user_id"]
    decade=content["Era"]
    userid=str(userid)
    cur.execute("select exists(select user_id from user_questions where user_id="+userid+");")
    check=cur.fetchone()
    c=0
    language=""
    if 0 in check:
        c=0
    else:
        c=1
    
    if region[0]==1:
        language="English"
    elif region[0]==2:
        language="Hindi"
    else:
        return "Enter valid Region"

    coins_reduce=int(noq)*2
    cur.execute("UPDATE achievements SET u_coins = u_coins - "+str(coins_reduce)+" WHERE user_id="+userid+";")
    try:
        #Question
        if str(isr) == "True" or str(isr)=="true":
            if c==1:
                if len(region)==2:
                    cur.execute("SELECT image.image_URL as \"ImgUrl\",movie.movie_id as \"Qid\", movie.m_title as \"Answer\" FROM movie JOIN image ON movie.movie_id=image.movie_id join lang on movie.movie_id=lang.movie_id AND movie.movie_id not in (SELECT GROUP_CONCAT(qid) FROM user_questions WHERE user_id="+userid+") GROUP BY movie.movie_id order by rand() limit "+str(noq)+";")
                else:
                    cur.execute("SELECT image.image_URL as \"ImgUrl\",movie.movie_id as \"Qid\", movie.m_title as \"Answer\" FROM movie JOIN image ON movie.movie_id=image.movie_id join lang on movie.movie_id=lang.movie_id where lang.m_lang=\""+language+"\" AND movie.movie_id not in (SELECT GROUP_CONCAT(qid) FROM user_questions WHERE user_id="+userid+") GROUP BY movie.movie_id order by rand() limit "+str(noq)+";")       
            else:
                if len(region)==2:
                    cur.execute("SELECT image.image_URL as \"ImgUrl\",movie.movie_id as \"Qid\", movie.m_title as \"Answer\" FROM movie JOIN image ON movie.movie_id=image.movie_id join lang on movie.movie_id=lang.movie_id GROUP BY movie.movie_id order by rand() limit "+str(noq)+";")
                else:
                    cur.execute("SELECT image.image_URL as \"ImgUrl\",movie.movie_id as \"Qid\", movie.m_title as \"Answer\" FROM movie JOIN image ON movie.movie_id=image.movie_id join lang on movie.movie_id=lang.movie_id where lang.m_lang=\""+language+"\" GROUP BY movie.movie_id order by rand() limit "+str(noq)+";")       

            row_headers = [x[0] for x in cur.description]  # this will extract row headers
            myresult = cur.fetchall()
            myresult=list(myresult)
            json_data=[]
            cid=[]
            ans=[]
            j=0
            for i in myresult:
                qresult=dict(zip(row_headers,i))
                cid.append(qresult["Qid"])
                ans.append(qresult["Answer"])

            #options
                if len(region)==1:
                    cur.execute("select aoptions.optext as \"options\" from aoptions join lang on lang.movie_id=aoptions.opid join movie on aoptions.opid=movie.movie_id where aoptions.opid!= \""+cid[j]+"\" and lang.m_lang=\""+language+"\" AND movie.m_release>\"1970\" AND movie.m_release<\"2020\" order by rand() limit 3;")
                else:
                    cur.execute("select aoptions.optext as \"options\" from aoptions join lang on lang.movie_id=aoptions.opid join movie on aoptions.opid=movie.movie_id where aoptions.opid!= \""+cid[j]+"\" and movie.m_release>\"1970\" AND movie.m_release<\"2020\" order by rand() limit 3;")
               
                opresult = cur.fetchall()
                opresult=list(opresult)
                opt1={'Name':'','Id':''}
                options=[]
                chars=[",","\"","\'"]
                i=0
                for x in opresult:
                    opt={'Name':'','Id':''}
                    x=str(x)
                    x=x.replace('(','')
                    x=x.replace(')','')
                    x=re.sub("|".join(chars),"",x)
                    opt['Name']=x
                    opt['Id']=i
                    options.append(opt)
                    i=i+1
                opt1['Name']=ans[j]
                opt1['Id']=4
                options.append(opt1)
                j=j+1        
                oprow_headers = [x[0] for x in cur.description]  # this will extract row headers
                qresult['options']=options
                del qresult['Answer']
                json_data.append(qresult)
            response={'IsSuccess':True,'error':'null','Data':[]}
            response['Data']=json_data
        else:
            start=[]
            end=[]
            def getStart(arg):
                switcher = {
                    "1": 1971,
                    "2": 1981,
                    "3": 1991,
                    "4": 2001,
                    "5": 2011
                }
                return switcher.get(str(arg),0)
            def getEnd(arg):
                switcher = {
                    "1": 1980,
                    "2": 1990,
                    "3": 2000,
                    "4": 2010,
                    "5": 2020
                }
                return switcher.get(arg,0)
            for d in decade:
                start.append(getStart(str(d)))
                end.append(getEnd(str(d)))
                
            
            if c==1:
                if len(region)==1:
                    cur.execute("SELECT image.image_URL as \"ImgUrl\",movie.movie_id as \"Qid\", movie.m_title as \"Answer\" FROM movie JOIN image ON movie.movie_id=image.movie_id join lang on movie.movie_id=lang.movie_id where lang.m_lang=\""+language+"\" AND movie.m_release>"+str(start[0])+" AND movie.m_release<"+str(end[-1])+" AND movie.movie_id not in (SELECT GROUP_CONCAT(qid) FROM user_questions WHERE user_id="+userid+") GROUP BY movie.movie_id order by rand() limit "+str(noq)+";")
                else:
                    cur.execute("SELECT image.image_URL as \"ImgUrl\",movie.movie_id as \"Qid\", movie.m_title as \"Answer\" FROM movie JOIN image ON movie.movie_id=image.movie_id join lang on movie.movie_id=lang.movie_id where movie.m_release>"+str(start[0])+" AND movie.m_release<"+str(end[-1])+" AND movie.movie_id not in (SELECT GROUP_CONCAT(qid) FROM user_questions WHERE user_id="+userid+") GROUP BY movie.movie_id order by rand() limit "+str(noq)+";")
            else:
                if len(region)==1:
                    cur.execute("SELECT image.image_URL as \"ImgUrl\",movie.movie_id as \"Qid\", movie.m_title as \"Answer\" FROM movie JOIN image ON movie.movie_id=image.movie_id join lang on movie.movie_id=lang.movie_id where lang.m_lang=\""+language+"\" AND movie.m_release>"+str(start[0])+" AND movie.m_release<"+str(end[-1])+" GROUP BY movie.movie_id order by rand() limit "+str(noq)+";")
                else:
                    cur.execute("SELECT image.image_URL as \"ImgUrl\",movie.movie_id as \"Qid\", movie.m_title as \"Answer\" FROM movie JOIN image ON movie.movie_id=image.movie_id join lang on movie.movie_id=lang.movie_id where movie.m_release>"+str(start[0])+" AND movie.m_release<"+str(end[-1])+" GROUP BY movie.movie_id order by rand() limit "+str(noq)+";")

            row_headers = [x[0] for x in cur.description]  # this will extract row headers
            myresult = cur.fetchall()
            myresult=list(myresult)
            json_data=[]
            cid=[]
            ans=[]
            j=0
            for i in myresult:
                qresult=dict(zip(row_headers,i))
                cid.append(qresult["Qid"])
                ans.append(qresult["Answer"])
            #options
                if len(region)==1:
                    cur.execute("select aoptions.optext as \"options\" from aoptions join lang on lang.movie_id=aoptions.opid join movie on aoptions.opid=movie.movie_id where aoptions.opid!= \""+cid[j]+"\" and lang.m_lang=\""+language+"\" AND movie.m_release>\""+str(start[0])+"\" AND movie.m_release<\""+str(end[-1])+"\" order by rand() limit 3;")
                else:
                    cur.execute("select aoptions.optext as \"options\" from aoptions join lang on lang.movie_id=aoptions.opid join movie on aoptions.opid=movie.movie_id where aoptions.opid!= \""+cid[j]+"\" and movie.m_release>\""+str(start[0])+"\" AND movie.m_release<\""+str(end[-1])+"\" order by rand() limit 3;")
                
                opresult = cur.fetchall()
                opresult=list(opresult)
                options=[]
                opt1={'Name':'','Id':''}
                i=0
                chars=[",","\"","\'"]
                for x in opresult:
                    opt={'Name':'','Id':''}
                    x=str(x)
                    x=x.replace('(','')
                    x=x.replace(')','')
                    x=re.sub("|".join(chars),"",x)
                    opt['Name']=x
                    opt['Id']=i
                    options.append(opt)
                    i=i+1
                opt1['Name']=ans[j]
                opt1['Id']=4
                options.append(opt1)
                j=j+1
                oprow_headers = [x[0] for x in cur.description]  # this will extract row headers
                qresult['options']=options
                del qresult['Answer']
                json_data.append(qresult)
            response={'IsSuccess':True,'error':'null','Data':[]}
            response['Data']=json_data
        return jsonify(response)
    except Exception as e:
        response={'IsSuccess':False,'error':str(e)}
        return jsonify(response)
   
   
    """#user status
        cur.execute("select usert.u_status as \"status\",achievements.u_coins as \"coins\" from usert join achievements on usert.user_id=achievements.user_id  where usert.user_id=\""+userid+"\"group by usert.user_id;")
        myresult1=cur.fetchall()
        row_headers1 = [x[0] for x in cur.description]  # this will extract row headers
        for x in myresult1:
            json_data.append(dict(zip(row_headers1,x)))"""


#Screen 8 & 9 Results API
@app.route('/result',methods=['POST'])
def viewResult():
    content=request.get_json()
    cur=connection.cursor()
    userid=content['userId']
    cc=content['Ccount']
    report=content['Report']
    qid=[]
    time=[]
    ic=[]
    c=0
    totalTime=0
    for x in report:
        qid.append(x['QID'])
        time.append(int(x['time']))
        ic.append(x['isCorrect']) 
    for x in time:
        totalTime=totalTime+x
        c=c+1  
    cans=int(cc)
    wans=len(ic)-cans
    totala=len(ic)
    avgtime=totalTime/c
    worsTime=max(time)
    bestTime=min(time)
    
    try:
        cur.execute("SELECT EXISTS(SELECT user_id FROM usert WHERE user_id="+userid+");")
        check=cur.fetchone()
        for x in check:
            print (x)
        if 0 in check:
            raise Exception("user does not exist")
        k=0
        for x in ic:
            if x==True:
                cur.execute("INSERT INTO user_questions (user_id,qid) VALUES("+userid+",\""+qid[k]+"\");")
                cur.execute("COMMIT")
            k=k+1
        
        cur.execute("select u_coins,correct_images,u_level,wrong_images from achievements WHERE user_id=\""+userid+"\";")
        row_headers = [x[0] for x in cur.description]  # this will extract row headers
        myresult = cur.fetchall()
        for i in myresult:
            result_dict=dict(zip(row_headers,i))
        level=int(result_dict['u_level'])
        coins=int(result_dict['u_coins'])
        ci=int(result_dict['correct_images'])
        wi=int(result_dict['wrong_images'])
        coinsperlevel=0
        coins_earned=0
        ci=ci+cans
        wi=wi+wans
        if level == 1:
            coinsperlevel=10
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=25 and coins>=1000:
                level=level+1
        elif level == 2:
            coinsperlevel=10
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=75 and coins>=2250:
                level=level+1
        elif level == 3:
            coinsperlevel=12
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=150 and coins>=4800:
                level=level+1
        elif level == 4:     
            coinsperlevel=12
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=225 and coins>=7200:
                level=level+1
        elif level == 5:
            coinsperlevel=15
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=350 and coins>=11750:
                level=level+1
        elif level == 6:
            coinsperlevel=15
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=450 and coins>=14750:
                level=level+1
        elif level == 7:
            coinsperlevel=15
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=550 and coins>=18250:
                level=level+1
        elif level == 8:   
            coinsperlevel=18
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=675 and coins>=24150:
                level=level+1     
        elif level == 9:
            coinsperlevel=18
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=800 and coins>=28400:
                level=level+1
        elif level == 10:
            coinsperlevel=18
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=1000 and coins>=34000:
                level=level+1
        elif level == 11: 
            coinsperlevel=20
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=1200 and coins>=44000:
                level=level+1
        elif level == 12:
            coinsperlevel=20
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=1400 and coins>=52000:
                level=level+1
        elif level == 13:
            coinsperlevel=22
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=1600 and coins>=65200:
                level=level+1
        elif level == 14:
            coinsperlevel=22
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=1800 and coins>=72600:
                level=level+1
        elif level == 15: 
            coinsperlevel=25
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=2000 and coins>=90000:
                level=level+1
        elif level == 16:
            coinsperlevel=25
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=2200 and coins>=100000:
                level=level+1
        elif level == 17:
            coinsperlevel=25
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=2400 and coins>=112000:
                level=level+1
        elif level == 18:
            coinsperlevel=28
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=2600 and coins>=132800:
                level=level+1
        elif level == 19:
            coinsperlevel=28
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=2800 and coins>=148400:
                level=level+1
        elif level == 20: 
            coinsperlevel=30
            coins_earned=coinsperlevel*cans
            coins=coins+coins_earned
            if ci>=3000 and coins>=180000:
                level=level+1

        cur.execute("UPDATE achievements SET u_level=\""+str(level)+"\",totalTime=\""+str(totalTime)+"\",u_coins=\""+str(coins)+"\",correct_images=\""+str(ci)+"\",wrong_images=wrong_images+\""+str(wi)+"\" WHERE user_id=\""+str(userid)+"\";")
        cur.execute("COMMIT")
        cur.execute("select u_level as \"level\" from achievements where user_id=\""+userid+"\"")
        myresult=cur.fetchall()
        row_headers = [x[0] for x in cur.description]
        res=[]
        for result in myresult:
            json_data=dict(zip(row_headers,result))
        json_data['coins']=coins_earned
        json_data['avgtime']=avgtime
        json_data['bestTime']=bestTime
        json_data['worstTime']=worsTime
        res.append(json_data)

        response={'IsSuccess':True,'error':'null','Data':res}
        return jsonify(response)
    except Exception as e:
        response={'IsSuccess':False,'error':str(e)}
        return jsonify(response)

#Screen 18 API link for Leaderboard Screen
@app.route('/leaderBoard/<userid>', methods=['GET'])
def leaderBoard(userid):
    try:
        cur=connection.cursor()
        #cur.execute("SELECT usert.user_name as \"NAME\", achievements.u_coins as \"Coins\", achievements.u_level as \"Level\", avatar.a_img_url as \"URL\" FROM usert JOIN achievements ON usert.user_id=achievements.user_id AND usert.region=\'"+region+"\' AND usert.city=\'"+city+"\' JOIN user_genre ON usert.user_id=user_genre.user_id AND user_genre.u_genre=\'"+genre+"\' JOIN avatar ON usert.avatar_id=avatar.avatar_id ORDER BY achievements.u_coins")
        cur.execute("SELECT user_id,u_level,u_coins,correct_images,wrong_images,totalTime from achievements where correct_images!=0")
        row_headers = [x[0] for x in cur.description]  # this will extract row headers
        myresult = cur.fetchall()
        json_data = []

        for result in myresult:
            json_data.append(dict(zip(row_headers, result)))
    
        user_id=[]
        level=[]
        coins=[]
        hitRatio=[]
        avgTime=[]

        for x in range(len(json_data)):
            user_id.append(int(json_data[x]['user_id']))
            level.append(int(json_data[x]['u_level']))
            coins.append(int(json_data[x]['u_coins']))
            correctQues=int(json_data[x]['correct_images'])
            wrongQues=int(json_data[x]['wrong_images'])
            totalQues=correctQues+wrongQues
            hRatio=correctQues/totalQues
            hitRatio.append(hRatio)
            timetotal=int(json_data[x]['totalTime'])
            avg_time=timetotal/totalQues
            avgTime.append(avg_time)
   
        #Sorting
        user=[]
        x=0
        n=len(user_id)
        while x<n:
            i=0
            j=1
            if len(level)>1:
                if level[i]<level[j]:
                    user.append(user_id[j])
                    level.remove(level[j])
                    coins.remove(coins[j])
                    hitRatio.remove(hitRatio[j])
                    avgTime.remove(avgTime[j])
                    user_id.remove(user_id[j])
                elif level[i]==level[j]:
                    if coins[i]<coins[j]:
                        user.append(user_id[j])
                        level.remove(level[j])
                        coins.remove(coins[j])
                        hitRatio.remove(hitRatio[j])
                        avgTime.remove(avgTime[j])
                        user_id.remove(user_id[j])
                    elif coins[i]==coins[j]:
                        if hitRatio[i]<hitRatio[j]:
                            user.append(user_id[j])
                            level.remove(level[j])
                            coins.remove(coins[j])
                            hitRatio.remove(hitRatio[j])
                            avgTime.remove(avgTime[j])
                            user_id.remove(user_id[j])
                        elif hitRatio[i]==hitRatio[j]:
                            if avgTime[i]>avgTime[j]:
                                user.append(user_id[j])
                                level.remove(level[j])
                                coins.remove(coins[j])
                                hitRatio.remove(hitRatio[j])
                                avgTime.remove(avgTime[j])
                                user_id.remove(user_id[j])
                            else:
                                user.append(user_id[i])
                                level.remove(level[i])
                                coins.remove(coins[i])
                                hitRatio.remove(hitRatio[i])
                                avgTime.remove(avgTime[i])
                                user_id.remove(user_id[i])
                        else:
                            user.append(user_id[i])
                            level.remove(level[i])
                            coins.remove(coins[i])
                            hitRatio.remove(hitRatio[i])                        
                            avgTime.remove(avgTime[i])
                            user_id.remove(user_id[i])
                    else:
                        user.append(user_id[i])
                        level.remove(level[i])
                        coins.remove(coins[i])
                        hitRatio.remove(hitRatio[i])
                        avgTime.remove(avgTime[i])
                        user_id.remove(user_id[i])
                else:
                    user.append(user_id[i])
                    level.remove(level[i])
                    coins.remove(coins[i])
                    hitRatio.remove(hitRatio[i])
                    avgTime.remove(avgTime[i])
                    user_id.remove(user_id[i])
            x=x+1
        data=[]
        for x in user:
            print(x)
        user_rank=user.index(int(userid))
        user_rank=user_rank+1
        print(userid)
        print(user_rank)
        if len(user)>20:
            for i in range(20):
                data_dict={'UserRank':i+1,'LeaderBoard':''}
                cur.execute("SELECT usert.screen_name as \"NAME\", achievements.u_coins as \"Coins\", achievements.u_level as \"Level\", usert.img_url as \"URL\" FROM usert JOIN achievements ON usert.user_id=achievements.user_id where usert.user_id=\""+str(user[i])+"\";")
                row_headers = [x[0] for x in cur.description]  # this will extract row headers
                myresult = cur.fetchall()
                jsondata = []
                for result in myresult:
                    jsondata.append(dict(zip(row_headers, result)))
                data_dict['LeaderBoard']=jsondata
                data.append(data_dict)
            data_dict={'UserRank':str(user_rank),'LeaderBoard':''}
            cur.execute("SELECT usert.screen_name as \"NAME\", achievements.u_coins as \"Coins\", achievements.u_level as \"Level\", usert.img_url as \"URL\" FROM usert JOIN achievements ON usert.user_id=achievements.user_id where usert.user_id=\""+str(userid)+"\";")
            row_headers = [x[0] for x in cur.description]  # this will extract row headers
            myresult = cur.fetchall()
            jsondata = []
            for result in myresult:
                jsondata.append(dict(zip(row_headers, result)))
            data_dict['LeaderBoard']=jsondata
            data.append(data_dict)
            response={'IsSuccess':True,'error':'null','Data':data}
            return jsonify(response)
        else:
            for i in range(len(user)):
                data_dict={'UserRank':i+1,'LeaderBoard':''}
                cur.execute("SELECT usert.screen_name as \"NAME\", achievements.u_coins as \"Coins\", achievements.u_level as \"Level\", usert.img_url as \"URL\" FROM usert JOIN achievements ON usert.user_id=achievements.user_id where usert.user_id=\""+str(user[i])+"\";")
                row_headers = [x[0] for x in cur.description]  # this will extract row headers
                myresult = cur.fetchall()
                jsondata = []
                for result in myresult:
                    jsondata.append(dict(zip(row_headers, result)))
                data_dict['LeaderBoard']=jsondata
                data.append(data_dict)
            data_dict={'UserRank':str(user_rank),'LeaderBoard':''}
            cur.execute("SELECT usert.screen_name as \"NAME\", achievements.u_coins as \"Coins\", achievements.u_level as \"Level\", usert.img_url as \"URL\" FROM usert JOIN achievements ON usert.user_id=achievements.user_id where usert.user_id=\""+str(userid)+"\";")
            row_headers = [x[0] for x in cur.description]  # this will extract row headers
            myresult = cur.fetchall()
            jsondata = []
            for result in myresult:
                jsondata.append(dict(zip(row_headers, result)))
            data_dict['LeaderBoard']=jsondata
            data.append(data_dict)
            response={'IsSuccess':True,'error':'null','Data':data}
            return jsonify(response)
    except Exception as e:
        response={'IsSuccess':False,'error':str(e)}
        return jsonify(response)   

#Screen 19 API link to earn coins through ads
@app.route('/earncoins',methods=['POST'])
def earncoins():
    cur=connection.cursor()
    content=request.get_json()
    userid=content["Id"]
    coins_add=content["CoinsToAdd"]
    resource_id=content["ResourceID"]
    credit=content["Credit"]
    debit=content["Debit"]
    try:
        if credit=="True":
            cur.execute("UPDATE achievements SET u_coins= u_coins + "+str(coins_add)+" WHERE user_id="+str(userid)+";")
            cur.execute("COMMIT;")
            cur.execute("UPDATE resources SET credit = credit + "+str(coins_add)+" WHERE rid="+str(resource_id)+";")
            cur.execute("COMMIT;")
            response={'IsSuccess':True,'error':'null'}
            return jsonify(response)
        elif debit=="True":
            cur.execute("UPDATE achievements SET u_coins= u_coins - "+str(coins_add)+" WHERE user_id="+str(userid)+";")
            cur.execute("COMMIT;")
            cur.execute("UPDATE resources SET debit = debit - "+str(coins_add)+" WHERE rid="+str(resource_id)+";")
            cur.execute("COMMIT;")
            response={'IsSuccess':True,'error':'null'}
            return jsonify(response)
    except Exception as e:
        response={'IsSuccess':False,'error':str(e)}
        return jsonify(response)

if __name__ == '__main__':
    app.run(debug=True,port=9999)
