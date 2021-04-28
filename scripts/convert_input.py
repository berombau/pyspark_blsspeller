from pathlib import Path
import re
import argparse
import sys

parser = argparse.ArgumentParser(
    description="Convert BLSpeller input files, changing Newick tree format from gene-centric to species-centric. Defaults to stdin and stdout."
)
parser.add_argument("-i", "--input", type=str, help="Input directory with files.")
parser.add_argument(
    "-o", "--output", type=str, help="Output directory to be created/overwritten."
)
args = parser.parse_args()


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


def act_on_input(f, o):
    family_id = f.readline().rstrip("\n")
    while family_id:
        print(family_id)
        count = 0
        tree = f.readline().rstrip("\n")
        num_genes = int(f.readline().rstrip("\n"))
        genes = []
        for _ in range(num_genes):
            gene_id, species_id = f.readline().rstrip("\n").split()
            sequence = f.readline().rstrip("\n")
            count += 1
            genes.append((gene_id, species_id, sequence))
        print(count)
        o.writelines(
            "\n".join(
                [
                    family_id,
                    convert_tree(tree, genes),
                    str(num_genes),
                ]
                + [
                    f"{gene_id}\t{species_id}\n{sequence}"
                    for gene_id, species_id, sequence in genes
                ]
            )
        )
        o.write("\n")
        family_id = f.readline().rstrip("\n")


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
                act_on_input(
                    f,
                )

    except Exception as e:
        print(f"Failed for file {file.name}. Caused by {e}")
