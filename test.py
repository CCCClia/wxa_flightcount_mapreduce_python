from collections import defaultdict
import csv

def main():
    # 读取数据文件
    with open('data/AComp_Passenger_data_no_error_DateTime.csv', 'r') as file:
        reader = csv.reader(file)
        counts = defaultdict(int)
        for row in reader:
            value = row[0]
            counts[value] += 1

        # 找出出现最多次数的值和其出现次数
        max_value = max(counts, key=counts.get)
        max_count = counts[max_value]
        print(f"出现最多次数的值是 '{max_value}'，共出现 {max_count} 次。")

if __name__ == "__main__":
    main()
