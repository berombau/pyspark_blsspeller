from pathlib import Path
import re
import argparse
import sys
import logging
import sys
from collections import defaultdict
import random

random.seed(123)

def convert_tree(tree, genes):
    """
    >>> convert_tree("(OB:0.2,(ZM_g:1.0E-6,ZM_g:1.0E-6):0.2);", [("ZM_g", "ZM", "")])
    '(OB:0.2,ZM:0.2);'
    >>> convert_tree("(OB:0.2,(ZM_g:1.0E-6,ZM_g:1.0E-6,ZM_g:1.0E-6):0.2);", [("ZM_g", "ZM", "")])
    '(OB:0.2,ZM:0.2);'
    """
    # replace gene names with species names
    # all paralogs map to same species
    for gene_id, species_id, _ in genes:
        tree = tree.replace(gene_id, species_id)
    # replace unbounded number of paralog nodes with single species node
    pattern = re.compile(r"\(([^:]+):([^,]+)(,\1:\2)+\)")
    tree = pattern.sub(r"\1", tree)
    return tree

def new_genes_format(genes):
    genes_by_species = defaultdict(lambda: ([], []))
    for gene_id, species_id, sequence in genes:
        ids, seqs = genes_by_species[species_id]
        ids.append(gene_id)
        seqs.append(sequence)
    return [
        f"{' '.join(ids)}\t{species_id}\n{' '.join(seqs)}"
        for species_id, (ids, seqs) in genes_by_species.items()
    ]

poss_dict = {
    "A" : "A",
    "C" : "C",
    "G" : "G",
    "T" : "T",
    "N" : "ACGT",
    "R" : "AG",
    "Y" : "CT",
    "S" : "GC",
    "W" : "AT",
    "K" : "GT",
    "M" : "AC",
    "B" : "CGT",
    "D" : "AGT",
    "H" : "ACT",
    "V" : "ACG"
}

def clean_seq(seq):
    """
    >>> clean_seq('ACGT') 
    'ACGT'
    >>> clean_seq('NNNN')
    'NNNN'
    >>> clean_seq('ACGKAA')
    'ACGGAA'
    """
    output = []
    for c in seq:
        if c in 'ACGTN':
            output.append(c)
        else:
            poss = poss_dict[c]
            choice = random.choice(poss)
            output.append(choice)
    return "".join(output)

def act_on_input(f, o):
    family_id = f.readline().rstrip("\n")
    while family_id:
        logging.info(family_id)
        tree = f.readline().rstrip("\n")
        num_genes = int(f.readline().rstrip("\n"))
        genes = []
        species = set()
        for _ in range(num_genes):
            gene_id, species_id = f.readline().rstrip("\n").split()
            sequence = f.readline().rstrip("\n")
            sequence = clean_seq(sequence)
            species.add(species_id)
            genes.append((gene_id, species_id, sequence))
        logging.info(species)
        o.writelines(
            "\n".join(
                [
                    family_id,
                    convert_tree(tree, genes),
                    str(len(species)),
                ]
                + new_genes_format(genes)
            ) + "\n"
        )
        family_id = f.readline().rstrip("\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert BLSpeller input files, changing Newick tree format from gene-centric to species-centric. Defaults to stdin and stdout."
    )
    parser.add_argument("-i", "--input", type=str, help="Input directory with files.")
    parser.add_argument(
        "-o", "--output", type=str, help="Output directory to be created/overwritten."
    )
    parser.add_argument(
        "-v", "--verbose", action='store_true', help="Set logging level to DEBUG."
    )
    args = parser.parse_args()

    if args.verbose:
        level=logging.DEBUG
    else:
        level=logging.WARNING

    logging.basicConfig(stream=sys.stderr, level=level)

    if not args.input or args.input == "-":
        # only 1 input, via std channels
        act_on_input(sys.stdin, sys.stdout)
        sys.exit()
    # assume input folder and output folder
    output = Path(args.output)
    if output.exists():
        for f in output.iterdir():
            f.unlink()
        output.rmdir()
    output.mkdir()

    for file in Path(args.input).iterdir():
        try:
            with file.open(mode="r") as f:
                output_path = output / file.name
                output_path.touch()
                with output_path.open(mode="w") as o:
                    act_on_input(f, o)

        except Exception as e:
            logging.error(f"Failed for file {file.name}. Caused by {e}")
