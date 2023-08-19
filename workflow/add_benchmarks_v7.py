#! usr/bin/env python

import argparse
import re
from multiprocessing import Process, Manager

def process_rule(rule_name, data, rule_data, benchmark_data):
    benchmark_section = f"\n    benchmark:\n        'benchmarks/{rule_name}.tsv'"
    #rule_pattern = rf'\nrule {rule_name}:((\n(.+))+)'
    target_rule_pattern = rf'(?<=\nrule\s){rule_name}:((\n(.+))+)\s+output:((\n(.+))+)(?=\s\s\s\s+shell:|\s\s\s\s+run:|\s\s\s\s+script:)'
    rule_match = re.search(target_rule_pattern, data)
    #print(rule_match)
    if rule_match:
        rule_text = rule_match.group(0)
        #print(rule_text)
        if 'benchmark:' not in rule_text:
            updated_rule_text = rule_text + benchmark_section
            #print(updated_rule_text)
            rule_data[rule_name] = rule_text
            benchmark_data[rule_name] = updated_rule_text
            print(f"Added benchmark section to rule '{rule_name}'")
        else:
            print(f"Benchmark section already exists for rule '{rule_name}'")
    else:
        print(f"Rule '{rule_name}' a top-level rule or not found in data")

def main():
    p = argparse.ArgumentParser()
    p.add_argument('-s', '--snakefile', help='The snakefile to bless with benchmark sections')
    p.add_argument('-t', '--top-level', help='The snakefile has top-level rules (e.g. rule all)')
    p.add_argument('-w', '--wildcards', help='Include the wildcards in benchmark file name')
    p.add_argument('-r', '--repeats', help='Set the amount of repeats for all benchmarks')
    p.add_argument('-o', '--output', help='New snakefile with benchmark sections')
    args = p.parse_args()

    snakefile = args.snakefile

    with open(snakefile, 'r') as fp:
        data = fp.read()

    all_rules = re.findall(r'rule (.+):', data)
    if all_rules:
        print("Found rules:", all_rules)

        rule_indices = {rule_name: idx for idx, rule_name in enumerate(all_rules)}
#        print(rule_indices)
        manager = Manager()

        rule_data = manager.dict()
        benchmark_data = manager.dict()

        processes = []

        for rule_name in all_rules:
            process = Process(target=process_rule, args=(rule_name, data, rule_data, benchmark_data))
            process.start() 
            processes.append(process)
#            print(process)
        for process in processes:
            process.join()
#            print(process)
        for rule_name, modified_rule_data in benchmark_data.items():
            if modified_rule_data:
                #rule_name = all_rules[idx]
                #print('zzz', (rule_name,), 'zzz2')
                #print('yyy', (modified_rule_data,), 'yyy2')
                rule_text = rule_data[rule_name]
                data = data.replace(rule_text, modified_rule_data)
#                print(data)
        with open(args.output, 'w') as output_fp:
            output_fp.write(data)

if __name__ == "__main__":
    main()
