#include <random>
#include <fstream>
#include "zhelpers.hpp"
using namespace std;
int main () {

    int sa=0,rand1,rand2;
    string  str="0,0,0,0,0,0,0,0";
    string symbol[5] = {"EUR_USD", "AUR_USD", "CAN_USD", "SNG_USD","INR_USD"}; 
    
    // open a file in write mode.
		ofstream outfile;
		outfile.open("/home/lk/Documents/worklog/Database/master_data.csv", ios::app);
    
    //  Prepare our context and publisher
    zmq::context_t context(1);
    zmq::socket_t publisher(context, ZMQ_PUB);
    publisher.bind("tcp://*:5553");
    usleep(300000);
    while (true) {
        //random number genration
        std::random_device rd; // obtain a random number from hardware
	    std::mt19937 eng(rd()); // seed the generator
		std::uniform_int_distribution<> distr(10000, 99999); // define the range
        for (int i = 0; i < 5; i++){
            rand1 = distr(eng);
            rand2 = distr(eng);
        
            // current date/time based on current system
            time_t now = time(0);

            //cout << "Number of sec since January 1,1900:" << ctime(&now) << endl;

            tm *ltm = localtime(&now);

            //1900 + ltm->tm_year << ',' << 1 + ltm->tm_mon << ',' << ltm->tm_mday << ',' << ltm->tm_hour << ',' << ltm->tm_min << ',' << ltm->tm_sec << endl;
            
            str = symbol[i]+","+std::to_string(rand1)+","+std::to_string(rand2)+","+std::to_string(1900 + ltm->tm_year)+","+std::to_string(1 + ltm->tm_mon)+","+std::to_string(ltm->tm_mday)+","+std::to_string(ltm->tm_hour)+","+std::to_string(ltm->tm_min)+","+std::to_string(ltm->tm_sec);
            
            //  Write two messages, each with an envelope and content
            outfile << str << endl;
            s_sendmore (publisher, "tickdata");
            s_send (publisher, str);
            // s_sendmore (publisher, "B");
            // s_send (publisher, "We would like to see this");

            std::cout << str << endl;
            
            sa++;
            
        }
        usleep(1000000);
    }
    publisher.close();
    context.close();
    return 0;
}
