from pipeline.orchestrator import _is_junk_project_name


def test_real_names_not_junk():
    for n in ["Hemi", "Karlawinda", "Rebecca-Roe", "15-Mile", "Winu", "Nova",
              "Sanbrado Gold", "Rhyolite Ridge", "The Granites", "Mount Isa",
              "Rhyolite Ridge Lithium-Boron",
              "Vulcan Zero Carbon Lithium Phase One"]:  # all proper nouns, 6 words OK
        assert _is_junk_project_name(n) is False, n


def test_fragment_names_are_junk():
    for n in ["the", "our", "each", "both", "it", "updated", "Updated",
              "our Duketon", "its Bellevue Gold", "later in the",
              "Further information regarding the", "reporting of",
              "both the Solomon and the Chichester",
              "continues to emerge as a new high-quality Australian gold",
              "resumption of commercial production at our US-based Lance",
              "Table 4 and the 2018 Annual",  # capitalised start but interior connectives
              "Fletcher Zone at the Beta Hunt"]:
        assert _is_junk_project_name(n) is True, n


def test_empty_is_junk():
    assert _is_junk_project_name("") is True
    assert _is_junk_project_name("   ") is True
