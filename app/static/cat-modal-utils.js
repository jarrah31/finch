// Shared mixin for the "Add New Category" modal.
// Used by both rules.html and categories.html.
// Spread into the Alpine component's return object:
//   return { ...window.catModalMixin, ...pageSpecificState };
//
// Requires the consuming component to have:
//   - this.categories  (array)
//   - this.newCatModal (object with: catSearch, catDropdownOpen, selectedParentId,
//                       highlightedId, highlightNew, dropdownY, dropdownX, dropdownW)
//   - this.$refs.newCatTopInput
//   - A 'new-cat-sub' input element in the DOM
//   - Page-specific: openNewCatModal(...) and saveNewCategory()
window.catModalMixin = {

  filteredTopLevelCats(search) {
    const cats = this.categories
      .filter(c => !c.parent_id)
      .sort((a, b) => (a.display_order - b.display_order) || a.name.localeCompare(b.name));
    if (!search) return cats;
    return cats.filter(c => c.name.toLowerCase().includes(search.toLowerCase()));
  },

  openTopCatDropdown() {
    const el = this.$refs.newCatTopInput;
    if (el) {
      const r = el.getBoundingClientRect();
      this.newCatModal.dropdownY = r.bottom + 3;
      this.newCatModal.dropdownX = r.left;
      this.newCatModal.dropdownW = r.width;
    }
    this.newCatModal.catDropdownOpen = true;
  },

  closeNewCatModal() {
    this.newCatModal.open = false;
  },

  exactCatTopMatch(search) {
    if (!search.trim()) return false;
    return this.categories
      .filter(c => !c.parent_id)
      .some(c => c.name.toLowerCase() === search.trim().toLowerCase());
  },

  updateCatHighlight() {
    const search = this.newCatModal.catSearch.trim();
    if (!search) { this.newCatModal.highlightedId = null; this.newCatModal.highlightNew = false; return; }
    const filtered = this.filteredTopLevelCats(search);
    if (filtered.length > 0) {
      this.newCatModal.highlightedId = filtered[0].id;
      this.newCatModal.highlightNew = false;
    } else {
      this.newCatModal.highlightedId = null;
      this.newCatModal.highlightNew = !this.exactCatTopMatch(search);
    }
  },

  navCatHighlight(dir) {
    const search = this.newCatModal.catSearch.trim();
    if (!search) return;
    const filtered = this.filteredTopLevelCats(search);
    const showNew = !this.exactCatTopMatch(search);
    // Virtual list: [null ("+New"), ...cat ids] or [...cat ids]
    const items = showNew ? [null, ...filtered.map(c => c.id)] : filtered.map(c => c.id);
    if (items.length === 0) return;
    let curIdx = this.newCatModal.highlightNew ? 0
      : items.indexOf(this.newCatModal.highlightedId);
    if (curIdx === -1) curIdx = dir > 0 ? -1 : items.length;
    const newIdx = Math.max(0, Math.min(items.length - 1, curIdx + dir));
    const item = items[newIdx];
    this.newCatModal.highlightNew = item === null;
    this.newCatModal.highlightedId = item !== null ? item : null;
  },

  acceptCatHighlight() {
    if (this.newCatModal.highlightedId) {
      const cat = this.categories.find(c => c.id === this.newCatModal.highlightedId);
      if (cat) {
        this.newCatModal.selectedParentId = cat.id;
        this.newCatModal.catSearch = cat.name;
        this.newCatModal.catDropdownOpen = false;
        this.newCatModal.highlightedId = null;
        this.$nextTick(() => document.getElementById('new-cat-sub').focus());
      }
    } else if (this.newCatModal.highlightNew) {
      this.newCatModal.catDropdownOpen = false;
      this.newCatModal.highlightNew = false;
      this.$nextTick(() => document.getElementById('new-cat-sub').focus());
    }
  },

};
