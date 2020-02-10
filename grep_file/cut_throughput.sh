#!bin/bash
sudo grep MessageType log.txt > all_line_throughput_cut.txt
sudo cat log.txt | grep MessageType | awk '{print $14,$15}' > only_data_throughput_result_cut.txt

