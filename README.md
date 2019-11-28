# MSSNG DB6 Extract Valid Chromosomes VCFs

Produce valid VCFs, one per chromosome. The `vcf_samples_input` is the `GVCFtyper_file` files from step 03 and the `vcf_main` input is either the `GVCFtyper_main` files also from step 03 or the `GVCFtyper_main.recal` recalibrated files from step 04 (depending on whether or not you want VQSR to be performed).
`region` is a string containing the chromosome name only (e.g. chr1). 
`all_samples` is a TSV file with a single line with all sample names separated by tabs - the final VCF will only have these samples in it.
`is_recalibrated` only influences the name of the output file, and refers to whether you're extracting valid VCFs from the base `GVCFtyper_main` file or the recalibrated file.
This will output one final, valid VCF per chromosome.