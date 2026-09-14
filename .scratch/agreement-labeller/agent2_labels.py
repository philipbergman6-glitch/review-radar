"""A second agent annotator's labels for the stratified 50 (NOT Philip's, NOT `human`).

Philip asked the agent to label the 50 directly. RR-21 condition 2 reserves
`label_source="human"` for him, so these are written to their own file under their own
annotator id and are never imported into the `human` slot. What they can support is a
*second-agent* consistency number against `agent_reference`; they cannot support
`THEMES_AGREEMENT`, which RR-21 defines as agent-vs-Philip and which stays NOT_RUN.

Labelling protocol actually followed, stated so a reader can discount it correctly:

- Pass A labelled every review from `eval/themes/blind-agreement-audit.jsonl` alone --
  `blind_id`, `title`, `text` -- plus `conf/theme-taxonomy.json`. No star rating, no product,
  no window, and no sight of `eval/themes/reference-audit.jsonl` (RR-21 condition 1).
- Pass B re-read each review adversarially against Pass A: is every labelled theme really a
  *complaint* about *this* product, is every `excludes` rule respected, and is any complaint
  missing? Changes are recorded in PASS_B_CHANGES below.
- Pass B is a verification pass, not an independent replication: Pass A was in context while
  it ran, so it catches errors but cannot estimate this annotator's own reproducibility.

`quote` strings are copied verbatim from the review; `build_agent2.py` re-derives every one
of them against the blind export and refuses to write a file if any is not an exact
whitespace-collapsed substring.
"""

# theme ids, for reference:
#   does_not_work, poor_build_quality, overpriced, wrong_size_or_fit, unpleasant_texture,
#   hard_to_use, unpleasant_scent, not_as_described, irritation_or_harm, arrived_damaged

LABELS: dict[str, dict] = {
    # --- no complaint: praise or neutral description only -------------------
    "aud-0005": {"themes": [], "sentiment": "positive", "confidence": "high",
                 "note": "praise throughout; 'No leakage' is the product succeeding"},
    "aud-0010": {"themes": [], "sentiment": "positive", "confidence": "medium",
                 "note": "'price is good' is explicit praise; the Ulta 3-pack aside is a "
                         "reservation, not a value complaint"},
    "aud-0014": {"themes": [], "sentiment": "positive", "confidence": "high"},
    "aud-0022": {"themes": [], "sentiment": "positive", "confidence": "high"},
    "aud-0029": {"themes": [], "sentiment": "positive", "confidence": "medium",
                 "note": "'Very sheer' reads descriptive, and every body sentence is positive"},
    "aud-0031": {"themes": [], "sentiment": "positive", "confidence": "high"},
    "aud-0034": {"themes": [], "sentiment": "positive", "confidence": "high"},
    "aud-0037": {"themes": [], "sentiment": "positive", "confidence": "high"},
    "aud-0045": {"themes": [], "sentiment": "positive", "confidence": "high"},
    "aud-0046": {"themes": [], "sentiment": "positive", "confidence": "high"},
    "aud-0051": {"themes": [], "sentiment": "positive", "confidence": "high"},
    "aud-0053": {"themes": [], "sentiment": "positive", "confidence": "high"},
    "aud-0055": {"themes": [], "sentiment": "positive", "confidence": "high"},
    "aud-0057": {"themes": [], "sentiment": "positive", "confidence": "high"},
    "aud-0075": {"themes": [], "sentiment": "positive", "confidence": "high"},
    "aud-0077": {"themes": [], "sentiment": "positive", "confidence": "high"},
    "aud-0078": {"themes": [], "sentiment": "positive", "confidence": "high"},
    "aud-0079": {"themes": [], "sentiment": "positive", "confidence": "high",
                 "note": "TRAP: 'burn or itch' is negated -- the product did neither"},
    "aud-0090": {"themes": [], "sentiment": "positive", "confidence": "high",
                 "note": "TRAP: the cracking and broken teeth are OTHER guards, not this one"},
    "aud-0115": {"themes": [], "sentiment": "positive", "confidence": "high"},
    "aud-0123": {"themes": [], "sentiment": "positive", "confidence": "high",
                 "note": "TRAP: 'way too soft' is off-brand washcloths, not this product"},
    "aud-0127": {"themes": [], "sentiment": "positive", "confidence": "high",
                 "note": "TRAP: heavy irritation vocabulary, all of it the medical condition; "
                         "the product is 'no irritation or burning whatsoever'"},
    "aud-0144": {"themes": [], "sentiment": "positive", "confidence": "high"},
    "aud-0159": {"themes": [], "sentiment": "positive", "confidence": "medium",
                 "note": "the scraping feel is raised and immediately dismissed ('used to it by "
                         "my very next shower') -- mentioned, not complained about"},
    "aud-0160": {"themes": [], "sentiment": "positive", "confidence": "high",
                 "note": "TRAP: itchy eyes are the pre-existing condition the product fixed"},
    "aud-0179": {"themes": [], "sentiment": "positive", "confidence": "high",
                 "note": "TRAP: the dryer that 'gave up' is the prior one"},
    "aud-0181": {"themes": [], "sentiment": "positive", "confidence": "medium",
                 "note": "the complaint is aimed at other reviewers' honesty, not the product"},

    # --- complaints ---------------------------------------------------------
    "aud-0042": {"themes": [("irritation_or_harm", "Didnt like how irritated  my skin felt")],
                 "sentiment": "negative", "confidence": "high",
                 "note": "title 'Chemically' is NOT labelled unpleasant_scent: the body never "
                         "mentions smell, so the word may describe the feel"},
    "aud-0066": {"themes": [], "sentiment": "positive", "confidence": "medium",
                 "note": "PASS B removed does_not_work. 'I only wish it retained heat for more "
                         "than 8 minutes' is immediately excused as universal to the category "
                         "('All the masks I've tried were the same'), so it is a limitation "
                         "noted, not a complaint about this product. The pilling is other brands."},
    "aud-0085": {"themes": [("arrived_damaged", "The box arrived smashed"),
                            ("not_as_described",
                             "a sticker slapped on the front stating expires Sept 2021")],
                 "sentiment": "negative", "confidence": "high"},
    "aud-0087": {"themes": [("unpleasant_scent", "also because of the strong scent")],
                 "sentiment": "mixed", "confidence": "medium",
                 "note": "the 'too strong baby powder' and the dryness are quoted from OTHER "
                         "reviews; only the parenthetical is this reviewer's own complaint"},
    "aud-0096": {"themes": [("does_not_work",
                             "My ear fell off with nothing touching or pressuring it"),
                            ("hard_to_use", "There were no instructions on how to use it")],
                 "sentiment": "negative", "confidence": "high"},
    "aud-0100": {"themes": [("unpleasant_scent", "the scent is horrible")],
                 "other": "no expiration date",
                 "sentiment": "negative", "confidence": "high",
                 "note": "the missing expiration date is given as a reason not to buy and fits "
                         "none of the ten"},
    "aud-0104": {"themes": [("wrong_size_or_fit",
                             "the opening to attach to hairdryers is way too small")],
                 "sentiment": "negative", "confidence": "high",
                 "note": "not does_not_work: wrong_size_or_fit owns 'does not fit the device it "
                         "is meant for'"},
    "aud-0107": {"themes": [("hard_to_use",
                             "There is a pretty big learning curve when using this for the first time")],
                 "sentiment": "mixed", "confidence": "medium",
                 "note": "framed constructively, but 'challenging' twice and a tip list for "
                         "getting it off is a usability complaint"},
    "aud-0118": {"themes": [("irritation_or_harm", "burned the hell out of my sac")],
                 "sentiment": "negative", "confidence": "high",
                 "note": "does_not_work deliberately NOT labelled: the product's own function "
                         "worked ('It took of the hair'); the soothing cream is a component"},
    "aud-0125": {"themes": [("arrived_damaged",
                             "half of the bottle had seeped out into the box")],
                 "sentiment": "negative", "confidence": "high"},
    "aud-0134": {"themes": [("does_not_work", "the nails come off even with glue"),
                            ("hard_to_use",
                             "the set confused me I feel it should of came with instructions")],
                 "sentiment": "mixed", "confidence": "high"},
    "aud-0148": {"themes": [("poor_build_quality", "the nail polishes were of such bad quality"),
                            ("hard_to_use",
                             "the consistency was so thick it seemed almos impossible to work with"),
                            ("not_as_described",
                             "The colors were supposed to be pastels according to the pictures")],
                 "sentiment": "negative", "confidence": "high",
                 "note": "the missing cable is folded into not_as_described (the listing said it "
                         "came with one) rather than given its own `other`"},
    "aud-0154": {"themes": [("unpleasant_scent", "does have a strong scent"),
                            ("irritation_or_harm",
                             "what's giving me headaches the last few days")],
                 "sentiment": "mixed", "confidence": "medium",
                 "note": "unpleasant_scent's excludes routes a smell-caused headache to "
                         "irritation_or_harm 'as well'; the causal claim is hedged, hence medium"},
    "aud-0163": {"themes": [("not_as_described", "They don't look like the picture advertisement")],
                 "sentiment": "negative", "confidence": "high",
                 "note": "the sheerness is the same grievance as the picture mismatch, so it is "
                         "not separately does_not_work"},
    "aud-0167": {"themes": [("not_as_described", "I was looking forward to a variety"),
                            ("overpriced", "I feel like I just wasted my money")],
                 "sentiment": "negative", "confidence": "medium",
                 "note": "'money wasted' is an explicit includes-line for overpriced"},
    "aud-0169": {"themes": [("wrong_size_or_fit", "A lot smaller than I had hoped for")],
                 "sentiment": "negative", "confidence": "high"},
    "aud-0170": {"themes": [("poor_build_quality", "It only lasted 2 uses and quit")],
                 "sentiment": "negative", "confidence": "high",
                 "note": "does_not_work excluded by its own rule ('worked and then failed later "
                         "-> poor_build_quality'). overpriced NOT labelled: 'out of a decent "
                         "amount of money' is collateral, and overpriced requires the complaint "
                         "to be about value rather than behaviour"},
    "aud-0171": {"themes": [("does_not_work", "the power switch does not work"),
                            ("hard_to_use",
                             "I almost destroyed this trying to get the back off to insert the batteries")],
                 "sentiment": "negative", "confidence": "high",
                 "note": "does_not_work owns 'an intact item that never worked at all', so the "
                         "dead switch is not separately poor_build_quality"},
    "aud-0173": {"themes": [("poor_build_quality",
                             "there were definitely scraggly ends that needed to be trimmed"),
                            ("wrong_size_or_fit",
                             "The wig didn't stay snug on my head despite adjusting the straps")],
                 "sentiment": "mixed", "confidence": "high"},
    "aud-0178": {"themes": [("does_not_work", "it does absolutely nothing")],
                 "sentiment": "negative", "confidence": "high"},
    "aud-0189": {"themes": [("does_not_work", "The polish is thin, doesn't cover"),
                            ("poor_build_quality", "either the product or brush are cheap"),
                            ("overpriced", "This is the most expensive nailpolish I've ever gotten")],
                 "sentiment": "negative", "confidence": "high",
                 "note": "overpriced is carried on its own: the review's frame is value-for-price, "
                         "not only behaviour"},
    "aud-0190": {"themes": [("does_not_work", "it hasn’t strengthened my nails at all"),
                            ("poor_build_quality", "this polish chips off easily")],
                 "other": "turns yellow then brown",
                 "sentiment": "negative", "confidence": "high",
                 "note": "the oxidising discolouration is a distinct unwanted effect fitting none "
                         "of the ten"},
    "aud-0196": {"themes": [("hard_to_use",
                             "the small brushes were just very dense and hard to use"),
                            ("unpleasant_texture", "They were very stiff and rigid")],
                 "sentiment": "mixed", "confidence": "medium",
                 "note": "both are labelled because the review complains about the stiffness on "
                         "the face ('kind of pokey') as well as about the usability"},
}

# Changes Pass B made to Pass A, kept so the second read is auditable rather than asserted.
PASS_B_CHANGES = [
    ("aud-0066", "removed does_not_work",
     ("the 8-minute heat complaint is retracted in the next sentence as true of every mask "
      "the reviewer has tried, and the review closes on continuing to buy it")),
    ("aud-0118", "kept does_not_work off",
     "re-checked: the hair removal worked, so labelling does_not_work would contradict the text"),
    ("aud-0148", "dropped other=missing cable",
     ("the cable was 'supposed to come with' per the listing, which not_as_described already "
      "covers; `other` is for complaints fitting none of the ten")),
    ("aud-0167", "dropped other=CVS sticker",
     ("ambiguous whether the sticker or the colour is the grievance; restraint over an "
      "uninterpretable `other`")),
    ("aud-0170", "removed overpriced",
     ("overpriced's definition requires the complaint to be about value rather than the "
      "product's behaviour; here the money is a consequence of the failure")),
    ("aud-0163", "removed does_not_work",
     "sheer/not vibrant is the same grievance as the picture mismatch, not a second theme"),
    ("aud-0159", "removed unpleasant_texture",
     ("the scraping feel is dismissed in the same sentence; complaint_only excludes a theme "
      "mentioned and set aside")),
    ("aud-0042", "kept unpleasant_scent off",
     "re-checked the one-word title 'Chemically' against the body, which never mentions smell"),
]
