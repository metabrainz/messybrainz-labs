DIGITAL_FORMATS = [
    (1  ,  "CD"),
    (3 , "SACD"),
    (6 , "MiniDisc"),
    (11 , "DAT"),
    (12 , "Digital Media"),
    (16 , "DCC"),
    (25 , "HDCD"),
    (26 , "USB Flash Drive"),
    (27 , "slotMusic"),
    (28 , "UMD"),
    (33 , "CD-R"),
    (34 , "8cm CD"),
    (35 , "Blu-spec CD"),
    (36 , "SHM-CD"),
    (37 , "HQCD"),
    (38 , "Hybrid SACD"),
    (39 , "CD+G"),
    (40 , "8cm CD+G"),
    (42 , "Enhanced CD"),
    (43 , "Data CD"),
    (44 , "DTS CD"),
    (45 , "Playbutton"),
    (46 , "Music Card"),
    (49 , "3.5\" Floppy Disk"),
    (57 , "SHM-SACD"),
    (60 , "CED"),
    (61 , "Copy Control CD"),
    (62 , "SD Card"),
    (63 , "Hybrid SACD (CD layer)"),
    (64 , "Hybrid SACD (SACD layer)"),
    (74 , "PlayTape"),
    (75 , "HiPac"),
    (76 , "Floppy Disk"),
    (77 , "Zip Disk"),
    (82 , "VinylDisc (CD side)"),
    (48 , "VinylDisc"),
]

VIDEO_FORMATS = [
    (2 , "DVD"),
    (4 , "DualDisc"),
    (5 , "LaserDisc"),
    (71 , "8\" LaserDisc"),
    (72 , "12\" LaserDisc"),
    (17 , "HD-DVD"),
    (18 , "DVD-Audio"),
    (19 , "DVD-Video"),
    (20 , "Blu-ray"),
    (22 , "VCD"),
    (23 , "SVCD"),
    (41 , "CDV"),
    (47 , "DVDplus"),
    (59 , "VHD"),
    (66 , "DualDisc (DVD-Video side)"),
    (65 , "DualDisc (DVD-Audio side)"),
    (67 , "DualDisc (CD side)"),
    (68 , "DVDplus (DVD-Audio side)"),
    (69 , "DVDplus (DVD-Video side)"),
    (70 , "DVDplus (CD side)"),
    (80 , "VinylDisc (DVD side)"),
    (79 , "Blu-ray-R"),
]

ANALOG_FORMATS = [
    (7 , "Vinyl"),
    (29 , "7\" Vinyl"),
    (30 , "10\" Vinyl"),
    (31 , "12\" Vinyl"),
    (10 , "Reel-to-reel"),
    (8 , "Cassette"),
    (9 , "Cartridge"),
    (78 , "8-Track Cartridge"),
    (13 , "Other"),
    (14 , "Wax Cylinder"),
    (15 , "Piano Roll"),
    (81 , "VinylDisc (Vinyl side)"),
    (21 , "VHS"),
    (24 , "Betamax"),
    (50 , "Edison Diamond Disc"),
    (51 , "Flexi-disc"),
    (52 , "7\" Flexi-disc"),
    (53 , "Shellac"),
    (54 , "10\" Shellac"),
    (55 , "12\" Shellac"),
    (56 , "7\" Shellac"),
    (58 , "Pathe disc"),
    (73 , "Phonograph record")
]

def output_list(id, formats):

    for format_id, format in formats:
        print("INSERT INTO mapping.format_sort values (%d, %s);" % (id, format_id))
        id += 1

    return id

print("drop table mapping.format_sort;")
print("create table mapping.format_sort ( format integer, sort integer );")
print("create index format_sort_format_ndx on mapping.format_sort(format);")
print("create index format_sort_sort_ndx on mapping.format_sort(sort);")

id = 1
id = output_list(id, DIGITAL_FORMATS)
id = output_list(id, VIDEO_FORMATS)
id = output_list(id, ANALOG_FORMATS)