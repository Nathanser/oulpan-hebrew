(() => {
  const HOLD_DELAY_MS = 300;
  const AUTO_SCROLL_EDGE = 110;
  const AUTO_SCROLL_STEP = 22;
  const MODES = {
    default: 'default',
    reorder: 'reorder',
    delete: 'delete'
  };

  function initCardBuilder(options = {}) {
    const container = document.getElementById(options.containerId || 'cards-container');
    const template = document.getElementById(options.templateId || 'card-template');
    const addBtn = document.getElementById(options.addButtonId || 'add-card-bottom');
    if (!container || !template || !addBtn) return;

    const form = options.formId ? document.getElementById(options.formId) : null;
    const banner = options.bannerId ? document.getElementById(options.bannerId) : null;
    const modeToggle = options.modeToggleId ? document.getElementById(options.modeToggleId) : null;
    const modeMenu = options.modeMenuId ? document.getElementById(options.modeMenuId) : null;
    const bulkBar = options.bulkBarId ? document.getElementById(options.bulkBarId) : null;
    const bulkDeleteBtn = options.bulkDeleteBtnId ? document.getElementById(options.bulkDeleteBtnId) : null;
    const bulkCount = options.bulkCountId ? document.getElementById(options.bulkCountId) : null;
    const bottomActions = document.querySelector('.list-bottom-actions');

    let dragState = null;
    let mode = MODES.default;
    const selectedCards = new Set();

    const showBanner = (msg) => {
      if (!banner || !msg) return;
      banner.textContent = msg;
      banner.style.display = 'block';
      setTimeout(() => { banner.style.display = 'none'; }, 2500);
    };

    const updateSelectLock = () => {
      const active = !!dragState || mode === MODES.reorder;
      document.body.classList.toggle('no-text-select', !!active);
      if (active && document.activeElement && document.activeElement !== document.body) {
        document.activeElement.blur();
      }
    };

    const updateOrder = () => {
      const cards = Array.from(container.querySelectorAll('.builder-card'));
      cards.forEach((card, index) => {
        card.dataset.cardIndex = index;
        const number = card.querySelector('.card-number');
        if (number) number.textContent = index + 1;
        card.querySelectorAll('[data-field]').forEach(input => {
          const field = input.getAttribute('data-field');
          input.name = `cards[${index}][${field}]`;
        });
      });
    };

    const setCardInputsFrozen = (card, frozen) => {
      card.querySelectorAll('.builder-fields input').forEach(input => {
        input.readOnly = frozen;
        input.classList.toggle('input-frozen', frozen);
      });
    };

    const clearSelections = () => {
      selectedCards.clear();
      container.querySelectorAll('[data-select-checkbox]').forEach(cb => {
        cb.checked = false;
        cb.indeterminate = false;
      });
    };

    const resetCardValues = (card) => {
      card.querySelectorAll('input[type="text"]').forEach(inp => inp.value = '');
      const hidden = card.querySelector('[data-field="id"]');
      if (hidden) hidden.value = '';
    };

    const deleteCards = (cardsToDelete) => {
      const toDelete = Array.from(cardsToDelete || []);
      if (!toDelete.length) return;
      const allCards = Array.from(container.querySelectorAll('.builder-card'));
      if (toDelete.length === allCards.length) {
        const keep = toDelete.shift();
        if (keep) {
          resetCardValues(keep);
          selectedCards.delete(keep);
        }
      }
      toDelete.forEach(card => {
        card.remove();
        selectedCards.delete(card);
      });
      updateOrder();
      updateBulkBar();
    };

    const updateBulkBar = () => {
      if (!bulkBar) return;
      bulkBar.classList.toggle('show', mode === MODES.delete);
      if (mode !== MODES.delete) return;
      const count = selectedCards.size;
      if (bulkCount) bulkCount.textContent = `${count} sélectionné${count > 1 ? 's' : ''}`;
      if (bulkDeleteBtn) {
        bulkDeleteBtn.disabled = count === 0;
        bulkDeleteBtn.classList.toggle('active', count > 0);
      }
    };

    const applyMode = () => {
      container.dataset.mode = mode;
      if (bottomActions) {
        bottomActions.classList.toggle('hide', mode !== MODES.default);
      }
      const cards = Array.from(container.querySelectorAll('.builder-card'));
      cards.forEach(card => {
        const checkbox = card.querySelector('[data-select-checkbox]');
        if (mode !== MODES.reorder) card.classList.remove('dragging');
        setCardInputsFrozen(card, mode !== MODES.default);
        if (checkbox) {
          checkbox.disabled = mode !== MODES.delete;
          if (mode !== MODES.delete) {
            checkbox.checked = false;
            selectedCards.delete(card);
          }
        }
      });
      if (mode !== MODES.delete) {
        clearSelections();
      }
      updateBulkBar();
      updateSelectLock();
    };

    const setMode = (next) => {
      const desired = next === MODES.reorder || next === MODES.delete ? next : MODES.default;
      mode = desired;
      if (modeToggle) {
        const isDefault = mode === MODES.default;
        modeToggle.innerHTML = isDefault ? '&#8230;' : '&times;';
        modeToggle.classList.toggle('is-active', !isDefault);
        modeToggle.setAttribute('aria-expanded', 'false');
      }
      closeLegacyMenus();
      if (modeMenu) modeMenu.classList.remove('show');
      applyMode();
    };

    const toggleCardSelection = (card, explicitValue) => {
      if (mode !== MODES.delete) return;
      const checkbox = card.querySelector('[data-select-checkbox]');
      if (!checkbox) return;
      const next = explicitValue !== undefined ? explicitValue : !checkbox.checked;
      checkbox.checked = next;
      if (next) {
        selectedCards.add(card);
      } else {
        selectedCards.delete(card);
      }
      updateBulkBar();
    };

    const removeSelectedCards = () => {
      if (mode !== MODES.delete) return;
      const count = selectedCards.size;
      if (!count) return;
      const confirmed = window.confirm(`Êtes-vous sûr de vouloir supprimer ${count} carte${count > 1 ? 's' : ''} ?`);
      if (!confirmed) return;
      deleteCards(Array.from(selectedCards));
    };

    const closeLegacyMenus = () => {
      container.querySelectorAll('.builder-card.legacy-open').forEach(card => {
        card.classList.remove('legacy-open');
        card.classList.remove('drag-ready');
        if (mode === MODES.default) {
          setCardInputsFrozen(card, false);
        }
      });
      updateSelectLock();
    };

    const handleAction = (card, action) => {
      if (action === 'delete') {
        deleteCards([card]);
      } else if (action === 'move-up') {
        const prev = card.previousElementSibling;
        if (prev) container.insertBefore(card, prev);
      } else if (action === 'move-down') {
        const next = card.nextElementSibling;
        if (next) container.insertBefore(next, card);
      } else if (action === 'add-below') {
        const newCard = createCard();
        container.insertBefore(newCard, card.nextElementSibling);
      }
      closeLegacyMenus();
      updateOrder();
      updateSelectLock();
    };

    const autoScroll = (clientY) => {
      if (clientY < AUTO_SCROLL_EDGE) {
        window.scrollBy({ top: -AUTO_SCROLL_STEP, behavior: 'auto' });
      } else if (clientY > window.innerHeight - AUTO_SCROLL_EDGE) {
        window.scrollBy({ top: AUTO_SCROLL_STEP, behavior: 'auto' });
      }
    };

    const placePlaceholder = (pageY) => {
      if (!dragState) return;
      const { card, placeholder } = dragState;
      const siblings = Array.from(container.querySelectorAll('.builder-card')).filter(el => el !== card);
      let placed = false;
      for (const sibling of siblings) {
        const rect = sibling.getBoundingClientRect();
        const middle = rect.top + window.scrollY + rect.height / 2;
        if (pageY < middle) {
          container.insertBefore(placeholder, sibling);
          placed = true;
          break;
        }
      }
      if (!placed) container.appendChild(placeholder);
    };

    const finishDrag = () => {
      if (!dragState) return;
      const { card, placeholder } = dragState;
      card.classList.remove('dragging');
      card.removeAttribute('style');
      container.insertBefore(card, placeholder);
      placeholder.remove();
      dragState = null;
      updateOrder();
      updateSelectLock();
      card.classList.add('drag-settled');
      setTimeout(() => card.classList.remove('drag-settled'), 300);
    };

    const startDrag = (card, startEvt) => {
      if (dragState || mode !== MODES.reorder) return;
      const rect = card.getBoundingClientRect();
      const placeholder = document.createElement('div');
      placeholder.className = 'card-placeholder';
      placeholder.style.height = `${rect.height}px`;
      dragState = {
        card,
        placeholder,
        offsetY: startEvt.clientY - rect.top,
        left: rect.left
      };
      container.insertBefore(placeholder, card.nextElementSibling);

      card.classList.add('dragging');
      updateSelectLock();
      card.style.width = `${rect.width}px`;
      card.style.height = `${rect.height}px`;
      card.style.left = `${rect.left}px`;
      card.style.top = `${rect.top}px`;
      card.style.position = 'fixed';
      card.style.pointerEvents = 'none';
      card.style.zIndex = '60';

      const onMove = (e) => {
        e.preventDefault();
        const clientY = e.clientY;
        if (!Number.isFinite(clientY)) return;
        card.style.top = `${clientY - dragState.offsetY}px`;
        card.style.left = `${dragState.left}px`;
        autoScroll(clientY);
        placePlaceholder(clientY + window.scrollY);
      };

      const onUp = () => {
        document.removeEventListener('pointermove', onMove);
        document.removeEventListener('pointerup', onUp);
        finishDrag();
      };

      document.addEventListener('pointermove', onMove);
      document.addEventListener('pointerup', onUp);
    };

    const attachDrag = (card) => {
      card.addEventListener('pointerdown', (e) => {
        if (mode !== MODES.reorder) return;
        if (!e.target.closest('[data-drag-handle]')) return;
        if (e.button !== 0 && e.pointerType !== 'touch') return;
        if (e.pointerType === 'touch') {
          e.preventDefault();
        }

        const startEvt = e;
        let moved = false;

        const detectMove = (moveEvt) => {
          if (Math.abs(moveEvt.clientY - startEvt.clientY) > 6 || Math.abs(moveEvt.clientX - startEvt.clientX) > 6) {
            moved = true;
          }
        };

        const cancel = () => {
          clearTimeout(timer);
          document.removeEventListener('pointermove', detectMove);
          document.removeEventListener('pointerup', cancel);
        };

        const timer = setTimeout(() => {
          if (!moved) startDrag(card, startEvt);
          cancel();
        }, HOLD_DELAY_MS);

        document.addEventListener('pointermove', detectMove);
        document.addEventListener('pointerup', cancel);
      });
    };

    const attachSelection = (card) => {
      const checkbox = card.querySelector('[data-select-checkbox]');
      if (checkbox) {
        checkbox.addEventListener('change', () => toggleCardSelection(card, checkbox.checked));
      }
      card.addEventListener('click', (e) => {
        if (mode !== MODES.delete) return;
        if (e.target.closest('[data-drag-handle]') || e.target.closest('.select-toggle')) return;
        e.preventDefault();
        toggleCardSelection(card);
      });
    };

    const attachLegacyActions = (card) => {
      const dots = card.querySelector('.dots-btn');
      if (dots) {
        dots.addEventListener('click', (e) => {
          e.preventDefault();
          e.stopPropagation();
          const open = !card.classList.contains('legacy-open');
          closeLegacyMenus();
          if (open) {
            card.classList.add('legacy-open');
            card.classList.add('drag-ready');
            setCardInputsFrozen(card, true);
          }
          updateSelectLock();
        });
      }
      card.querySelectorAll('[data-action]').forEach(btn => {
        btn.addEventListener('click', (e) => {
          e.preventDefault();
          e.stopPropagation();
          const action = btn.getAttribute('data-action');
          handleAction(card, action);
        });
      });
    };

    const createCard = () => {
      const clone = template.content.firstElementChild.cloneNode(true);
      attachDrag(clone);
      attachSelection(clone);
      attachLegacyActions(clone);
      return clone;
    };

    addBtn.addEventListener('click', () => {
      const newCard = createCard();
      container.appendChild(newCard);
      updateOrder();
      applyMode();
      newCard.scrollIntoView({ behavior: 'smooth', block: 'center' });
      const heb = newCard.querySelector('[data-field="hebrew"]');
      if (heb) setTimeout(() => heb.focus(), 180);
    });

    container.querySelectorAll('.builder-card').forEach(card => {
      attachDrag(card);
      attachSelection(card);
      attachLegacyActions(card);
    });

    if (modeToggle) {
      modeToggle.addEventListener('click', (e) => {
        e.preventDefault();
        if (mode !== MODES.default) {
          setMode(MODES.default);
          return;
        }
        if (modeMenu) {
          const open = modeMenu.classList.toggle('show');
          modeToggle.setAttribute('aria-expanded', open ? 'true' : 'false');
        }
      });
    }

    if (modeMenu) {
      modeMenu.querySelectorAll('[data-mode]').forEach(btn => {
        btn.addEventListener('click', (e) => {
          e.preventDefault();
          const chosen = btn.getAttribute('data-mode');
          setMode(chosen);
        });
      });
    }

    if (bulkDeleteBtn) {
      bulkDeleteBtn.addEventListener('click', (e) => {
        e.preventDefault();
        removeSelectedCards();
      });
    }

    document.addEventListener('click', (e) => {
      if (modeMenu && !e.target.closest('.title-input-row') && !e.target.closest('.list-mode-btn')) {
        modeMenu.classList.remove('show');
        if (modeToggle) modeToggle.setAttribute('aria-expanded', 'false');
      }
      if (!e.target.closest('.builder-card')) {
        closeLegacyMenus();
      }
    });

    updateOrder();
    applyMode();

    if (form) {
      form.addEventListener('submit', (e) => {
        if (!form.reportValidity()) {
          e.preventDefault();
          const invalid = form.querySelector(':invalid');
          if (invalid) {
            invalid.scrollIntoView({ behavior: 'smooth', block: 'center' });
            invalid.focus({ preventScroll: true });
          }
          showBanner('Complète les champs obligatoires.');
        }
      });
    }
  }

  window.initCardBuilder = initCardBuilder;
})();
