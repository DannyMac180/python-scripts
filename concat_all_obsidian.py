# Python script which takes all the files in your Obsidian Vault and concatenates into a single text file
import os

def traverse_vault(vault_path, output_file):
    with open(output_file, 'w', encoding='utf-8') as outfile:
        for root, dirs, files in os.walk(vault_path):
            for file in files:
                if file.endswith('.md') or file.endswith('.txt'):
                    file_path = os.path.join(root, file)
                    with open(file_path, 'r', encoding='utf-8') as infile:
                        content = infile.read()
                        outfile.write(content + '\n\n')

# Specify the path to your Obsidian MD vault
vault_path = '/Users/danielmcateer/Library/Mobile Documents/iCloud~md~obsidian/Documents/Ideaverse'

# Specify the output file name
output_file = 'all_dm_notes.txt'

# Call the function to traverse the vault and write the data
traverse_vault(vault_path, output_file)

print(f"Data from the Obsidian MD vault has been written to {output_file}.")
