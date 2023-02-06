"""
    This Flask web app provides a very simple dashboard to visualize the statistics sent by the spark app.
    The web app is listening on port 5000.
    All apps are designed to be run in Docker containers.

"""


from flask import Flask, jsonify, request, render_template
from redis import Redis
import matplotlib.pyplot as plt
import json

#my imports:
from datetime import datetime

app = Flask(__name__)

@app.route('/updateData', methods=['POST'])
def updateData():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('data', json.dumps(data))

    return jsonify({'msg': 'success'})

@app.route('/updateBatchData', methods=['POST'])
def updateData2():
    data = request.get_json()
    r = Redis(host='redis', port=6379)
    r.set('batchdata', json.dumps(data))

    if not r.exists('plotData'): #if it doesnt exist then create 3 empty values per key (language)
        r.set('plotData', json.dumps({'py': [], 'ch': [], 'ja': [], 'time': []}))
    else:
        
        try:
            batchdata = r.get('batchdata')
            batchdata = json.loads(batchdata)

            plotData = r.get('plotData')
            plotData = json.loads(plotData)


            tempdict = {}
            for item in batchdata:
                lang_freq = json.loads(item)
                # print('zzzzbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb')
                tempdict[lang_freq["_1"]] = lang_freq["_2"]
                # print(tempdict[lang_freq["_1"]])
                # print('zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz')

            # totalRepos = plotData
            currentUTCtime = datetime.utcnow().strftime("%H:%M:%S")

            if len(plotData['py']) < 5: #can append
                plotData['py'].append(tempdict['PYTHON'])
                plotData['ch'].append(tempdict['CSHARP'])
                plotData['ja'].append(tempdict['JAVA'])
                plotData['time'].append(currentUTCtime)

            else: #need to remove oldest, i.e first element
                plotData['py'].pop(0)
                plotData['ch'].pop(0)
                plotData['ja'].pop(0)
                plotData['time'].pop(0)
                plotData['py'].append(tempdict['PYTHON'])
                plotData['ch'].append(tempdict['CSHARP'])
                plotData['ja'].append(tempdict['JAVA'])
                plotData['time'].append(currentUTCtime)

            totalRepos = plotData
            r.set('plotData', json.dumps(plotData))
        except:
            print("AHHHHHHHHHHH ERROR")

    return jsonify({'msg': 'success'})

@app.route('/', methods=['GET'])
def index():
    r = Redis(host='redis', port=6379)
    data = r.get('data')
    batchdata = r.get('batchdata')
    python_wordFreq = []
    csharp_wordFreq = []
    java_wordFreq = []
    totalRepos = ""
    stargazerData = [0,0,0]

    ## r.set('python', json.dumps({'arr': []}))
    # pythonBatchData = r.get('python')
    # csharpBatchData = r.get('csharp')
    # javaBatchData = r.get('java')
    # timeBatchData = r.get('time')
    plotData = r.get('plotData')

    try:
        data = json.loads(data)
    except TypeError:
        # return "waiting for data..." # orig
        pass
    try:
        batchdata = json.loads(batchdata)
    except TypeError:
        # return "waiting for data..." # orig
        pass
    try:
        plotData = json.loads(plotData)
    except TypeError:
        # return "waiting for data..." # orig
        pass

    # 9 multiples: {'iv': {'word': ['for', 'and', 'the', 'to', 'of', 'a', 'in', 'A', 'with', 'is'], 'frequency': [208, 152, 137, 127, 108, 84, 82, 76, 59, 54]}, 
    # 'i_python': {'total_python_repos': [893]}, 'i_csharp': {'total_csharp_repos': [261]}, 'i_java': {'total_java_repos': [585]}, 'iii_python': {'avg_python_stargazer': [66.29003359462486]}, 'iii_csharp': {'avg_csharp_stargazer': [5.885057471264368]}, 'iii_java': {'avg_java_stargazer': [63.62735042735043]}}
    # non-9 multiples: ['{"_1":"CSHARP","_2":50}', '{"_1":"JAVA","_2":50}', '{"_1":"PYTHON","_2":50}']

    try:

        totalRepos = ["Python: " + str(data['i_python']['total_python_repos'][0]), "CSharp: " + str(data['i_csharp']['total_csharp_repos'][0]), "Java: " + str(data['i_java']['total_java_repos'][0])]

    except:
        totalRepos = ["Waiting", "For", "Data..."]
    

    try:
        # print('-------------------')
        # print(data)
        # print('----------------------') iv_python
        for i in range(0, len(data['iv_python']['python_words'])):
            python_wordFreq.append(data['iv_python']['python_words'][i] + ", " + str(data['iv_python']['frequency'][i]))

        for i in range(0, len(data['iv_csharp']['csharp_words'])):
            csharp_wordFreq.append(data['iv_csharp']['csharp_words'][i] + ", " + str(data['iv_csharp']['frequency'][i]))

        for i in range(0, len(data['iv_java']['java_words'])):
            java_wordFreq.append(data['iv_java']['java_words'][i] + ", " + str(data['iv_java']['frequency'][i]))
        # print(wordFreq)
        # print( len(data['iv']['word']))
        # data = len(data['iv']['word'])
    except:
        # python_wordFreq = "ERRROR"
        print('WORD FREQ EEEEEEEEEEROOOOOOOOOOOORRRRRRRRR')

    try:
        # data['i_python']['total_python_repos'][0]
        stargazerData = [data['iii_python']['avg_python_stargazer'][0], data['iii_csharp']['avg_csharp_stargazer'][0], data['iii_java']['avg_java_stargazer'][0]]
        # print('starrrrrrrrrrrrrrrrrrrrrrrrrr')
        # print(stargazerData[0])
        # print(stargazerData[1])
        # print(stargazerData[2])
    except:
        print("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")

    # try:
    #     yes_index = data['isMultipleOf9'].index('Yes')
    #     isMultipleOf9 = data['count'][yes_index]
    # except ValueError:
    #     isMultipleOf9 = 0
    try:
        # yes_index = data['isMultipleOf9'].index('Yes')
        isMultipleOf9 = data
    except ValueError:
        isMultipleOf9 = 0
    # try:
    #     notMultipleOf9 = data['count'][1 - yes_index]
    # except NameError:
    #     notMultipleOf9 = 0
    try:
        notMultipleOf9 = batchdata
    except NameError:
        notMultipleOf9 = 0
    # x = [1, 2]
    # height = [isMultipleOf9, notMultipleOf9]
    # tick_label = ['isMultipleOf9', 'notMultipleOf9']
    # plt.bar(x, height, tick_label=tick_label, width=0.8, color=['tab:orange', 'tab:blue'])
    # plt.ylabel('Count')
    # plt.xlabel('Type')
    # plt.title('Distribution of numbers')

    try:
        plt.clf()
        plt.plot(plotData['time'], plotData['py'], 'g', label='Python')
        plt.plot(plotData['time'], plotData['ch'], 'r', label='CSharp')
        plt.plot(plotData['time'], plotData['ja'], 'b', label='Java')
        plt.ylabel('#repositories')
        plt.xlabel('Time')
        plt.legend()
        # # plt.savefig('/app/nine-multiples/webapp/static/images/chart.png') #orig
        plt.savefig('/streaming/webapp/static/images/chart.png')
    except:
        pass
    
    try:
        plt.clf()
        x = ["Python", "CSharp", "Java"]
        height = stargazerData
        # tick_label = ['isMultipleOf9', 'notMultipleOf9']
        # plt.bar(x, height, tick_label=tick_label, width=0.8, color=['tab:orange', 'tab:blue', 'tab:red'])
        plt.bar(x, height, width=0.8, color=['tab:orange', 'tab:blue', 'tab:red'])
        plt.ylabel('Average number of stars')
        plt.xlabel('PL')
        # plt.title('Distribution of numbers')
        plt.savefig('/streaming/webapp/static/images/chartbar.png')
    except:
        pass
    return render_template('index.html', url='/static/images/chart.png',  url2='/static/images/chartbar.png', python_wordFreq=python_wordFreq, csharp_wordFreq=csharp_wordFreq, java_wordFreq=java_wordFreq, totalRepos=totalRepos)

if __name__ == '__main__':
    app.debug = True
    app.run(host='0.0.0.0')
