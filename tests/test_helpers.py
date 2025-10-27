from utils.helpers import normalize_symbol

# (Die typos in den Funktionsnamen sind egal, pytest stört das nicht)
def test_normilze_symbol_removes_space():
    assert normalize_symbol(" DAX ") == "DAX"
    assert normalize_symbol("Dow Jones") == "DowJones"

def test_normaliue_symbol_returns_emtpy_on_none():
    assert normalize_symbol(None) == ""