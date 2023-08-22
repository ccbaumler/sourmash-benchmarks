#! usr/bin/env python

import argparse
import re
from multiprocessing import Process, Manager

#create function for --resources that can be pulled process_rule

def process_rule(rule_name, data, rule_data, output_data, benchmark_data, wildcard_data, benchmark_count, top_level_rules):

    target_rule_pattern = rf'(?<=\nrule\s){rule_name}:((\n(.+))+)\s+output:((\n(.+))+)(?=\s\s\s\s+shell:|\s\s\s\s+run:|\s\s\s\s+script:)'
    rule_match = re.search(target_rule_pattern, data)
    
    output_pattern = r'(?<=output:)([\s\S]*?)(?=\n\s*\w+:|\Z)'
    wildcard_pattern = r'([{]{1,2}.+?[}]{1,2})' # internal parentheses arounf (.+?) will create a list of unbraced {} values 

    if rule_match:
        rule_text = rule_match.group(0)
        output_section = re.search(output_pattern, rule_text)
        wildcards = set(re.findall(wildcard_pattern, output_section.group(0)))
        print((wildcards,), )
        benchmark_wildcards = '.'.join(f"{wc}" for wc in wildcards)
        if benchmark_count==1 and 'benchmark:' not in rule_text:
            if any('{{' in wc for wc in wildcards):
                formatted_benchmark_section = f"\n    benchmark:\n        f'benchmarks/{rule_name}.{benchmark_wildcards}.tsv'"
                updated_rule_text = rule_text + formatted_benchmark_section
                rule_data[rule_name] = rule_text
                benchmark_data[rule_name] = updated_rule_text
                wildcard_data[rule_name] = wildcards
                print(f"Added benchmark section to rule '{rule_name} with {benchmark_count} repeat/s")
            else:
                benchmark_section = f"\n    benchmark:\n        'benchmarks/{rule_name}.{benchmark_wildcards}.tsv'"
                updated_rule_text = rule_text + benchmark_section
                rule_data[rule_name] = rule_text
                benchmark_data[rule_name] = updated_rule_text
                print(f"Added benchmark section to rule '{rule_name} with {benchmark_count} repeat/s")
        elif isinstance(benchmark_count, int) and 'benchmark:' not in rule_text:
            if any('{{' in wc for wc in wildcards):
                formatted_benchmark_section =  f"\n    benchmark:\n        repeat(f'benchmarks/{rule_name}{benchmark_wildcards}.tsv', {benchmark_count})"
                updated_rule_text = rule_text + formatted_benchmark_section
                rule_data[rule_name] = rule_text
                benchmark_data[rule_name] = updated_rule_text
                print(f"Added benchmark section to rule '{rule_name} with {benchmark_count} repeat/s")
            else:
                benchmark_section =  f"\n    benchmark:\n        repeat('benchmarks/{rule_name}{benchmark_wildcards}.tsv', {benchmark_count})"
                update_rule_text = rule_text + benchmark_section
                rule_data[rule_name] = rule_text
                benchmark_data[rule_name] = update_rule_text
                print(f"Added benchmark section to rule '{rule_name} with {benchmark_count} repeat/s")
        elif 'benchmark:' in rule_text:
            print(f"Benchmark section already exists for rule '{rule_name}'")
        else:
            print("Done!")
    else:
        top_level_rules.append(rule_name)



#https://stackoverflow.com/questions/14117415/how-can-i-constrain-a-value-parsed-with-argparse-for-example-restrict-an-integ
def check_positive(value):
    try:
        int_value = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError("%s is not an integer" % value)

    if int_value <= 0:
        raise argparse.ArgumentTypeError("%s is not a positive integer" % value)
    return int_value


def main():
    p = argparse.ArgumentParser(prog='name', description='{name} will do what I describe here')
    
    p.add_argument('-s', '--snakefile', required=True, help='The snakefile to bless with benchmark/resource sections')
    p.add_argument('-o', '--output', required=True, help='New snakefile with benchmark sections')
    p.add_argument('-t', '--top-level', action='store_true', help='Print the snakefile top-level/psuedo-rules rules (e.g. rule all)')
    p.add_argument('-w', '--wildcards', action='store_true', help='Print the wildcards in benchmark file name for each rule')
    p.add_argument('-b', '--benchmarks', type=check_positive, nargs='?', const=1, metavar='1,2,3, or ...', help='''Add benchmark sections to each target rule in the snakefile.
            Set any number of repeats for all benchmarks with a positive integer''')
    p.add_argument('-r', '--resources', help='Add resources section to each target rule in snakefile from benchmark output')
    args = p.parse_args()
    
    print(args.benchmarks)
    benchmark_count = args.benchmarks
    snakefile = args.snakefile
    top_level = args.top_level
    wildcard = args.wildcards

    with open(snakefile, 'r') as fp:
        data = fp.read()

    all_rules = re.findall(r'rule (.+):', data)
    if all_rules:
        print("Found rules:", all_rules)

        rule_indices = {rule_name: idx for idx, rule_name in enumerate(all_rules)}
        
        manager = Manager()

        rule_data = manager.dict()
        output_data = manager.dict()
        benchmark_data = manager.dict()
        wildcard_data = manager.dict()
        top_level_rules = manager.list()
        processes = []

        for rule_name in all_rules:
            process = Process(target=process_rule, args=(rule_name, data, rule_data, output_data, benchmark_data, wildcard_data, benchmark_count, top_level_rules))
            process.start() 
            processes.append(process)
        
        for process in processes:
            process.join()

        if top_level and top_level_rules:
            for rule in top_level_rules:
                print(f"Rule '{rule}' is a top-level/psuedo rule")
        elif top_level and not top_level_rules:
            print("No top-level/psuedo rules found!")

        if wildcard and wildcard_data:
            for rule_name, wildcards in wildcard_data.items():
                print(f"Wildcards for '{rule_name}': {wildcards}")
        elif wildcard and not wildcard_data:
            print("No wildcards found!")

        for rule_name, modified_rule_data in benchmark_data.items():
            if modified_rule_data:
                #rule_name = all_rules[idx]
                #print('zzz', (rule_name,), 'zzz2')
                #print('yyy', (modified_rule_data,), 'yyy2')
                rule_text = rule_data[rule_name]
                data = data.replace(rule_text, modified_rule_data)

        with open(args.output, 'w') as output_fp:
            output_fp.write(data)

if __name__ == "__main__":
    main()
