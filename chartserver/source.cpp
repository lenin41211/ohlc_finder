#include "zhelpers.hpp"
#include <iostream>
#include <ctime>
#include <random>
#include <map>
#include <vector>
#include <fstream>
#include <sstream>
#include <unistd.h>
#include <array>
#include <climits>

using namespace std;

namespace Con
{
	zmq::context_t context(1);
	zmq::socket_t publisher(context, ZMQ_PUB);
    void Func() {
		
		publisher.bind("tcp://*:5432");
		usleep(300000);
	}
}
class chartserver
{
    public:

		string hostname,port,path;

        map< array<int, 7>, string> tickdata[30];
        
        string openbuy, closebuy, opensell, closesell,tick,word,data,symbol[30]={"EUR_USD","AUR_USD","CAN_USD","SNG_USD","INR_USD"};
        array<int,7> key;
        int time_flag,size,tick_flag[30],highbuy , lowbuy  , highsell , lowsell  , high, low;

    chartserver(){
		path="/home/lk/Documents/worklog/Database/";
		hostname = "192.168.1.101";
		port = "5553";
        time_flag = 0;
        highbuy = -2147483648;
        lowbuy = 2147483647;
        highsell = -2147483648;
        lowsell = 2147483647;  
        size = 5;
		
		for(int i=0;i<size;i++){
			tick_flag[i]=0;
		}

		Con::Func();

    }

	void fileohlc()
	{
		vector<std::string> vec;
        // open a file in write mode.
		ifstream infile;
		infile.open(path+"master_data.csv");
		cout<<"File Reading :"<<endl;
        while (infile) {

            // Read a Line from File 
			getline(infile, data);
			//add data to vector
			std::stringstream ss(data);

            while (getline(ss, word, ',')) {
				vec.push_back(word);
			}
			if (vec.size() > 0) {

                tick = tick = vec.at(1) + "," + vec.at(2);
				key = { std::stoi(vec.at(3)),std::stoi(vec.at(4)),std::stoi(vec.at(5)),std::stoi(vec.at(6)),std::stoi(vec.at(7)),std::stoi(vec.at(8)) };
				
                for(int i=0;i<size;i++){
                    if(vec[0].compare(symbol[i]) == 0){
						                  
                        if((std::stoi(vec.at(8)) % 60 ) == time_flag){
							if(tickdata[i].size() != 0){
								//cout<<"1 minute"<<endl;
								ohlc(tickdata[i],"1",vec[0]);
								tickdata[i].clear();

								if((std::stoi(vec.at(7)) % 5 ) == time_flag)
								{
								//	cout<<"5 minute"<<endl;
									read_ohlc(vec[0],"1",5);

									if((std::stoi(vec.at(7)) % 15 ) == time_flag)
									{
								//		cout<<"15 minute"<<endl;
										read_ohlc(vec[0],"5",3);
										if((std::stoi(vec.at(7)) % 30 ) == time_flag)
										{
								//			cout<<"30 minute"<<endl;
											read_ohlc(vec[0],"15",2);
											if((std::stoi(vec.at(7)) % 60 ) == time_flag)
											{
								//				cout<<"60 minute  1 hour"<<endl;
												read_ohlc(vec[0],"30",2);
												if((std::stoi(vec.at(6)) % 3 ) == time_flag)
												{
								//						cout<<"180 minute 3 hour"<<endl;
														read_ohlc(vec[0],"60",3);
														if((std::stoi(vec.at(6)) % 6 ) == time_flag)
														{
								//								cout<<"360 minute 6 hour"<<endl;
																read_ohlc(vec[0],"180",2);
														}
												}
												if((std::stoi(vec.at(6)) % 2 ) == time_flag)
												{
								//					cout<<"120 minute  2 hour"<<endl;
													read_ohlc(vec[0],"60",2);
													if((std::stoi(vec.at(6)) % 4 ) == time_flag)
													{
								//						cout<<"240 minute 4 hour"<<endl;
														read_ohlc(vec[0],"120",2);
														if((std::stoi(vec.at(6)) % 8 ) == time_flag)
														{
								//							cout<<"480 minute 8 hour"<<endl;
															read_ohlc(vec[0],"240",2);
															if( std::stoi(vec.at(6)) == 0 && std::stoi(vec.at(7)) == 0 && std::stoi(vec.at(8)) == 0 )
															{
								//								cout<<"1440 minute 24 hour  1 day"<<endl;
																read_ohlc(vec[0],"480",3);
																tick_flag[i]++;
																if(tick_flag[i] == 5 )
																{
								//									cout<<"7200 minute 1 week"<<endl;
																	read_ohlc(vec[0],"1440",5);
																	tick_flag[i] = 0;
																}

																if((std::stoi(vec.at(5))) == 1)
																{
																	int nday;
								//									cout<<" 1 month"<<endl;
																	switch (std::stoi(vec.at(4)))
																	{
																		case 1:
																			nday = numofbussinessdays(31,std::stoi(vec[4]),std::stoi(vec[3]));
																			read_ohlc(vec[0],"1440",31-nday);
																			break;
																		case 2:
																		{
																			int nd;
																			int yr=std::stoi(vec.at(3));
																			if((yr%400)== 0){
																				nd = 29;
																			}
																			else if((yr%100)==0)
																			{
																				nd = 28;
																			}
																			else if((yr%4)==0)
																			{
																				nd = 29;
																			}
																			else
																			{
																				nd = 28;
																			}
																			nday = numofbussinessdays(nd,std::stoi(vec[4]),std::stoi(vec[3]));
																			read_ohlc(vec[0],"1440",nd-nday);
																		}
																		case 3:
																			nday = numofbussinessdays(31,std::stoi(vec[4]),std::stoi(vec[3]));
																			read_ohlc(vec[0],"1440",31-nday);
																			break;
																		case 4:
																			nday = numofbussinessdays(30,std::stoi(vec[4]),std::stoi(vec[3]));
																			read_ohlc(vec[0],"1440",30-nday);
																			break;
																		case 5:
																			nday = numofbussinessdays(31,std::stoi(vec[4]),std::stoi(vec[3]));
																			read_ohlc(vec[0],"1440",31-nday);
																			break;
																		case 6:
																			nday = numofbussinessdays(30,std::stoi(vec[4]),std::stoi(vec[3]));
																			read_ohlc(vec[0],"1440",30-nday);
																			break;
																		case 7:
																			nday = numofbussinessdays(31,std::stoi(vec[4]),std::stoi(vec[3]));
																			read_ohlc(vec[0],"1440",31-nday);
																			break;
																		case 8:
																			nday = numofbussinessdays(31,std::stoi(vec[4]),std::stoi(vec[3]));
																			read_ohlc(vec[0],"1440",31-nday);
																			break;
																		case 9:
																			nday = numofbussinessdays(30,std::stoi(vec[4]),std::stoi(vec[3]));
																			read_ohlc(vec[0],"1440",30-nday);
																			break;
																		case 10:
																			nday = numofbussinessdays(31,std::stoi(vec[4]),std::stoi(vec[3]));
																			read_ohlc(vec[0],"1440",31-nday);
																			break;
																		case 11:
																			nday = numofbussinessdays(30,std::stoi(vec[4]),std::stoi(vec[3]));
																			read_ohlc(vec[0],"1440",30-nday);
																			break;
																		case 12:
																			nday = numofbussinessdays(31,std::stoi(vec[4]),std::stoi(vec[3]));
																			read_ohlc(vec[0],"1440",31-nday);
																			break;
																		default:
																			break;
																	}
																}
															}
														}
													}
												}
											}
										}
									}
								}
							}
                        }
						tickdata[i][key]=tick;
                    }
                }
            }

            vec.clear();
        }
		infile.close();
    }

    void subscriber(){
    
		vector<std::string> vec;

        //  Prepare our context and publisher
		zmq::context_t context(1);
    	zmq::socket_t subscriber (context, ZMQ_SUB);
    	subscriber.connect("tcp://"+hostname+":"+port);
    	subscriber.setsockopt( ZMQ_SUBSCRIBE,"tickdata",1);
		cout<<"Subscriber :"<<endl;
        while (true) {
			
            //  Read envelope with address
			std::string address = s_recv (subscriber);
			//  Read message contents
			std::string contents = s_recv (subscriber);
			//cout<<contents<<endl;
			std::stringstream ss(contents);
			
			while (getline(ss, word, ',')) {
				vec.push_back(word);
			}
			if (vec.size() > 0) {

                tick = tick = vec.at(1) + "," + vec.at(2);
				key = { std::stoi(vec.at(3)),std::stoi(vec.at(4)),std::stoi(vec.at(5)),std::stoi(vec.at(6)),std::stoi(vec.at(7)),std::stoi(vec.at(8)) };

                for(int i=0;i<size;i++){
                    if(vec[0].compare(symbol[i]) == 0){
                      
                        if((std::stoi(vec.at(8)) % 60 ) == time_flag){
							if(tickdata[i].size() != 0){
								//cout<<"1 minute"<<endl;
								ohlc(tickdata[i],"1",vec[0]);
								tickdata[i].clear();

								if((std::stoi(vec.at(7)) % 5 ) == time_flag)
								{
								//	cout<<"5 minute"<<endl;
									read_ohlc(vec[0],"1",5);

									if((std::stoi(vec.at(7)) % 15 ) == time_flag)
									{
								//		cout<<"15 minute"<<endl;
										read_ohlc(vec[0],"5",3);
										if((std::stoi(vec.at(7)) % 30 ) == time_flag)
										{
								//			cout<<"30 minute"<<endl;
											read_ohlc(vec[0],"15",2);
											if((std::stoi(vec.at(7)) % 60 ) == time_flag)
											{
								//				cout<<"60 minute  1 hour"<<endl;
												read_ohlc(vec[0],"30",2);
												if((std::stoi(vec.at(6)) % 3 ) == time_flag)
												{
								//						cout<<"180 minute 3 hour"<<endl;
														read_ohlc(vec[0],"60",3);
														if((std::stoi(vec.at(6)) % 6 ) == time_flag)
														{
								//								cout<<"360 minute 6 hour"<<endl;
																read_ohlc(vec[0],"180",2);
														}
												}
												if((std::stoi(vec.at(6)) % 2 ) == time_flag)
												{
								//					cout<<"120 minute  2 hour"<<endl;
													read_ohlc(vec[0],"60",2);
													if((std::stoi(vec.at(6)) % 4 ) == time_flag)
													{
								//						cout<<"240 minute 4 hour"<<endl;
														read_ohlc(vec[0],"120",2);
														if((std::stoi(vec.at(6)) % 8 ) == time_flag)
														{
								//							cout<<"480 minute 8 hour"<<endl;
															read_ohlc(vec[0],"240",2);
															if( std::stoi(vec.at(6)) == 0 && std::stoi(vec.at(7)) == 0 && std::stoi(vec.at(8)) == 0 )
															{
								//								cout<<"1440 minute 24 hour  1 day"<<endl;
																read_ohlc(vec[0],"480",3);
																tick_flag[i]++;
																if(tick_flag[i] == 5 )
																{
								//									cout<<"7200 minute 1 week"<<endl;
																	read_ohlc(vec[0],"1440",5);
																	tick_flag[i] = 0;
																}
																if((std::stoi(vec.at(5))) == 1)
																{
																	int nday;
																	cout<<" 1 month"<<endl;
																	switch (std::stoi(vec.at(4)))
																	{
																		case 1:
																			nday = numofbussinessdays(31,std::stoi(vec[4]),std::stoi(vec[3]));
																			read_ohlc(vec[0],"1440",31-nday);
																			break;
																		case 2:
																		{
																			int nd;
																			int yr=std::stoi(vec.at(3));
																			if((yr%400)== 0){
																				nd = 29;
																			}
																			else if((yr%100)==0)
																			{
																				nd = 28;
																			}
																			else if((yr%4)==0)
																			{
																				nd = 29;
																			}
																			else
																			{
																				nd = 28;
																			}
																			nday = numofbussinessdays(nd,std::stoi(vec[4]),std::stoi(vec[3]));
																			read_ohlc(vec[0],"1440",nd-nday);
																		}
																		case 3:
																			nday = numofbussinessdays(31,std::stoi(vec[4]),std::stoi(vec[3]));
																			read_ohlc(vec[0],"1440",31-nday);
																			break;
																		case 4:
																			nday = numofbussinessdays(30,std::stoi(vec[4]),std::stoi(vec[3]));
																			read_ohlc(vec[0],"1440",30-nday);
																			break;
																		case 5:
																			nday = numofbussinessdays(31,std::stoi(vec[4]),std::stoi(vec[3]));
																			read_ohlc(vec[0],"1440",31-nday);
																			break;
																		case 6:
																			nday = numofbussinessdays(30,std::stoi(vec[4]),std::stoi(vec[3]));
																			read_ohlc(vec[0],"1440",30-nday);
																			break;
																		case 7:
																			nday = numofbussinessdays(31,std::stoi(vec[4]),std::stoi(vec[3]));
																			read_ohlc(vec[0],"1440",31-nday);
																			break;
																		case 8:
																			nday = numofbussinessdays(31,std::stoi(vec[4]),std::stoi(vec[3]));
																			read_ohlc(vec[0],"1440",31-nday);
																			break;
																		case 9:
																			nday = numofbussinessdays(30,std::stoi(vec[4]),std::stoi(vec[3]));
																			read_ohlc(vec[0],"1440",30-nday);
																			break;
																		case 10:
																			nday = numofbussinessdays(31,std::stoi(vec[4]),std::stoi(vec[3]));
																			read_ohlc(vec[0],"1440",31-nday);
																			break;
																		case 11:
																			nday = numofbussinessdays(30,std::stoi(vec[4]),std::stoi(vec[3]));
																			read_ohlc(vec[0],"1440",30-nday);
																			break;
																		case 12:
																			nday = numofbussinessdays(31,std::stoi(vec[4]),std::stoi(vec[3]));
																			read_ohlc(vec[0],"1440",31-nday);
																			break;
																		default:
																			break;
																	}			
																}
															}
														}
													}	
												}
											}
										}
									}
								}
							}    
                        }
						
						tickdata[i][key]=tick;
						
						zmq_ohlc(tickdata[i],vec[0]);
                    }
                }
            }

            vec.clear();  
        }        
    }

	void ohlc(map< array<int, 7>, string> sym,string folder,string fname){
       
        int count=1,s=1,b=1,x=1;
        string tempstr,time;
		
		highbuy = -2147483648;
        lowbuy = 2147483647;
        highsell = -2147483648;
        lowsell = 2147483647;  

		// open a file in write mode.
		ofstream outfile_buy;
		ofstream outfile_sell;
		outfile_buy.open(path+folder+"/"+fname+"_buy.csv",ios::app);
		outfile_sell.open(path+folder+"/"+fname+"_sell.csv",ios::app);
		
        for (auto itr = sym.begin(); itr != sym.end(); ++itr) {
            if (count == 1)
			{
				std::stringstream ss(itr->second);
				while (getline(ss, word, ',')) {
					tempstr = word;
			    	if (b == 1)
					{
						openbuy = tempstr;
					}
					if (b == 2)
					{
						opensell = tempstr;
					}
					b++;
				}
				time = std::to_string(itr->first[0])+','+std::to_string(itr->first[1])+','+std::to_string(itr->first[2])+','+std::to_string(itr->first[3])+','+std::to_string(itr->first[4])+','+std::to_string(itr->first[5]);
            }
						
			if (count == sym.size())
			{
				
				std::stringstream ss(itr->second);
				while (getline(ss, word, ',')) {
					tempstr = word;
					if (s == 1)
					{
						closebuy = tempstr;
					}
					if (s == 2)
					{
						closesell = tempstr;
					}
					s++;
			    }
			}

            std::stringstream ss(itr->second);
			while (getline(ss, word, ',')) {
				tempstr = word;
				if (x == 1)
				{
					std::stringstream ss(tempstr);
					ss >> high;
					std::stringstream s(tempstr);
					s >> low;
					
					if (highbuy < high)
					{
						
						highbuy = high;
					}
					
					if (lowbuy > low)
					{
						
						lowbuy = low;
					}
				}
				if (x == 2)
				{
					std::stringstream ss(tempstr);
					ss >> high;
					std::stringstream s(tempstr);
					s >> low;
					
					if (highsell < high)
					{
						
						highsell = high;
					}
					
					if (lowsell > low)
					{
							
						lowsell = low;
					}
				}
				x++;
			}
			x = 1;			
			count++;
        }
        
        outfile_buy <<time<<','<<openbuy<<','<<highbuy<<','<<lowbuy<<','<<closebuy<<endl;
		outfile_sell <<time<<','<<opensell<<','<<highsell<<','<<lowsell<<','<<closesell<<endl;						
        sym.clear();
		outfile_buy.close();
		outfile_sell.close();
    
    }
	
	void read_ohlc(string filename,string folder,int duration){
	
		map< array<int, 6>, array<int,4> > sym;
			
		std::vector<std::string> file_vec;
		vector<std::string> vec;

		array<int, 4> ohlcdata;
		array<int, 6> ohlckey;
		
		int length=0,t_flag=1,i=1;
		
		ifstream infile;

		while(i<=2){

			if(i==1)
			{
		
				infile.open(path+folder+"/"+filename+"_sell.csv");
				
			}
			if(i==2)
			{
		
				infile.open(path+folder+"/"+filename+"_buy.csv");
				
			}

			//cout<<folder<<"  "<<filename<<"  "<<duration<<endl;

			while (infile)
			{
				getline(infile,data);
				auto it  = file_vec.begin();
				file_vec.insert(it, data);
				
			}
			infile.close();
			
			while (length<duration) {
				
				std::stringstream ss(file_vec.at(duration-length));
	
				while (getline(ss, word, ',')) {
				
					vec.push_back(word);
				
				}
				if (vec.size() > 0) {
					
					ohlcdata = {std::stoi(vec.at(6)),std::stoi(vec.at(7)),std::stoi(vec.at(8)),std::stoi(vec.at(9)) };
					
					ohlckey = { std::stoi(vec.at(0)),std::stoi(vec.at(1)),std::stoi(vec.at(2)),std::stoi(vec.at(3)),std::stoi(vec.at(4)),std::stoi(vec.at(5)) };
					
					sym[ohlckey]=ohlcdata;

					if(t_flag==duration){
						if(i==1){
							ohlc_ohlc(sym,std::to_string(duration*std::stoi(folder)),filename+"_sell");
						}
						if(i==2){
							ohlc_ohlc(sym,std::to_string(duration*std::stoi(folder)),filename+"_buy");
						}
						sym.clear();
						t_flag=0;
					}
				}
				t_flag++;
				length++;
				vec.clear();
			}
			file_vec.clear();
			length=0;
			t_flag=1;
			i++;
		}
		i=0;
	}

	void ohlc_ohlc(map< array<int, 6>, array<int,4> > sym,string folder,string fname){

		int open,close,high=-2147483648,low=2147483647;
		int count = 1;
		string time;
		ofstream outfile;
		outfile.open(path+folder+"/"+fname+".csv",ios::app);

		for (auto itr = sym.begin(); itr != sym.end(); ++itr) {

			if(count==1)
			{
				open=itr->second[0];
				time = std::to_string(itr->first[0])+','+std::to_string(itr->first[1])+','+std::to_string(itr->first[2])+','+std::to_string(itr->first[3])+','+std::to_string(itr->first[4])+','+std::to_string(itr->first[5]);
			}
			if(count == sym.size()){
				close = itr->second[3];				
			}
			if(itr->second[1] > high){
				high=itr->second[1];
			}
			if(itr->second[2] < low){
				low=itr->second[2];
			}
			count++;

		}
		//cout<<folder<<"  "<<fname<<" "<<time<<','<<open<<','<<high<<','<<low<<','<<close<<endl;
		outfile <<time<<','<<open<<','<<high<<','<<low<<','<<close<<endl;
	}
	void zmq_ohlc(map< array<int, 7>, string> sym,string symbol){
	
		int count=1,s=1,b=1,x=1;
        string tempstr,time,ohlc_sell,ohlc_buy;
		
		highbuy = -2147483648;
        lowbuy = 2147483647;
        highsell = -2147483648;
        lowsell = 2147483647;  

        for (auto itr = sym.begin(); itr != sym.end(); ++itr) {
            if (count == 1)
			{
				std::stringstream ss(itr->second);
				while (getline(ss, word, ',')) {
					tempstr = word;
			    	if (b == 1)
					{
						openbuy = tempstr;
					}
					if (b == 2)
					{
						opensell = tempstr;
					}
					b++;
				}
				
            }
			time = std::to_string(itr->first[0])+','+std::to_string(itr->first[1])+','+std::to_string(itr->first[2])+','+std::to_string(itr->first[3])+','+std::to_string(itr->first[4])+','+std::to_string(itr->first[5]);			
			if (count == sym.size())
			{
				
				std::stringstream ss(itr->second);
				while (getline(ss, word, ',')) {
					tempstr = word;
					if (s == 1)
					{
						closebuy = tempstr;
					}
					if (s == 2)
					{
						closesell = tempstr;
					}
						s++;
			    }
			}

            std::stringstream ss(itr->second);
			while (getline(ss, word, ',')) {
				tempstr = word;
				if (x == 1)
				{
					std::stringstream ss(tempstr);
					ss >> high;
					std::stringstream s(tempstr);
					s >> low;
					
					if (highbuy < high)
					{
						
						highbuy = high;
					}
					
					if (lowbuy > low)
					{
						
						lowbuy = low;
					}

				}
				if (x == 2)
				{
					std::stringstream ss(tempstr);
					ss >> high;
					std::stringstream s(tempstr);
					s >> low;
					
					if (highsell < high)
					{
						
						highsell = high;
					}
					
					if (lowsell > low)
					{
							
						lowsell = low;
					}
					
				}
				x++;
			}
			x = 1;			
			count++;
        }

        ohlc_buy = time+","+openbuy+","+std::to_string(highbuy)+","+std::to_string(lowbuy)+","+closebuy;
		ohlc_sell = time+","+opensell+","+std::to_string(highsell)+","+std::to_string(lowsell)+","+closesell;
		s_sendmore (Con::publisher, "ohlc_sell_"+symbol);
        s_send (Con::publisher, ohlc_sell);
		s_sendmore (Con::publisher, "ohlc_buy_"+symbol);
        s_send (Con::publisher, ohlc_buy);
		cout<<symbol+"_buy : "<<time+","+openbuy+","+std::to_string(highbuy)+","+std::to_string(lowbuy)+","+closebuy<<endl;
		cout<<symbol+"_sell : "<<time+","+opensell+","+std::to_string(highsell)+","+std::to_string(lowsell)+","+closesell<<endl;
		sym.clear();	
	}
	int numofbussinessdays(int ndays,int month,int year){
		
		int count=0;

		if (month == 1 || month == 2) {
        month +=  12;
        year -= 1;
    	}
		
		for(int i=1;i<=ndays;i++){
			int day=((i + (int)floor((13 * (month + 1)) / 5) + year%100 + (int)floor((year%100)/ 4) + (int)floor(((int)floor(year/100))/4) + 5*(int)floor(year/100)) % 7);
			if(day == 0){
				count++;
			}
			if(day == 1){
				count++;
			}
		}
		return count;
	}
};

int main(){

    int sa;
	chartserver s;
	
	s.fileohlc();
	s.subscriber();

	cin >> sa;
	return 0;
}