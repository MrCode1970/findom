# Google Sheets Design — `ledger_first`

## 1) Цель варианта

`ledger_first` строит витрину вокруг принципа «главная сущность — нормализованный журнал операций (ledger)».

Источник истины: лист `Normalized` (только воспроизводимые поля из коннектора + вычисляемые служебные колонки).

## 2) Листы и назначение

### `Transactions` (raw, опционально)
Сырые события от провайдера (как staging-слой для диагностики).

**Колонки (A:H):**
1. `raw_external_id`
2. `raw_date`
3. `raw_amount`
4. `raw_currency`
5. `raw_account`
6. `raw_category_hint`
7. `raw_description`
8. `ingested_at`

> Лист не является источником истины, нужен для аудита преобразования raw → normalized.

---

### `Normalized` (ledger, главный лист)
Единый нормализованный журнал операций.

**Колонки (A:M):**
1. `external_id` *(обязательный, уникальный)*
2. `date` *(YYYY-MM-DD)*
3. `amount` *(расход < 0, доход/возврат > 0)*
4. `currency`
5. `account`
6. `category`
7. `description`
8. `month` *(вычисляется: первый день месяца)*
9. `direction` *(вычисляется: expense/income)*
10. `is_refund` *(вычисляется: TRUE/FALSE)*
11. `abs_amount` *(вычисляется: модуль суммы)*
12. `category_group` *(lookup из `Categories`)*
13. `notes_auto` *(вычисляемая служебная метка качества данных)*

**Формулы (строка 2, протянуть вниз / ARRAYFORMULA):**
- `H2` (`month`):
  ```gs
  =IF(B2="",,DATE(YEAR(B2),MONTH(B2),1))
  ```
- `I2` (`direction`):
  ```gs
  =IF(C2="",,IF(C2<0,"expense","income"))
  ```
- `J2` (`is_refund`):
  ```gs
  =IF(A2="",,REGEXMATCH(LOWER(G2),"refund|возврат|reversal"))
  ```
- `K2` (`abs_amount`):
  ```gs
  =IF(C2="",,ABS(C2))
  ```
- `L2` (`category_group`):
  ```gs
  =IF(F2="",,IFNA(VLOOKUP(F2,Categories!A:B,2,FALSE),"unmapped"))
  ```
- `M2` (`notes_auto`):
  ```gs
  =IF(A2="",,IF(COUNTIF($A:$A,A2)>1,"duplicate_external_id",IF(L2="unmapped","category_missing","ok")))
  ```

**Валидации:**
- `A:A` уникальность через условное форматирование.
- `D:D` список валют (`ILS`, `USD`, `EUR`).
- `F:F` только из `Categories!A:A` (data validation list-from-range).

---

### `Categories` (справочник)
Справочник категорий для стабильной категоризации и сводной аналитики.

**Колонки (A:D):**
1. `category` *(ключ, используется в `Normalized.category`)*
2. `category_group` *(high-level группа)*
3. `budget_type` *(fixed/variable)*
4. `is_active` *(TRUE/FALSE)*

**Базовый набор категорий:**
- groceries
- transport
- dining
- subscriptions
- utilities
- healthcare
- salary
- transfer
- cashback
- refund
- shopping
- travel
- entertainment
- fees

---

### `Dashboard` (сводка)
Автоматически собираемая витрина без ручного ввода.

**Блоки:**
1. `Monthly P&L`
   - Доходы: `=SUMIFS(Normalized!C:C,Normalized!H:H,$A2,Normalized!C:C,">0")`
   - Расходы: `=SUMIFS(Normalized!C:C,Normalized!H:H,$A2,Normalized!C:C,"<0")`
   - Net: `=B2+C2`
2. `Top expense categories (selected month)`
   - QUERY/PIVOT по `Normalized` с фильтром `direction="expense"`.
3. `Refund impact`
   - `refund_total`: `=SUMIFS(Normalized!C:C,Normalized!J:J,TRUE)`
   - `refund_count`: `=COUNTIFS(Normalized!J:J,TRUE)`
4. `Data quality`
   - `duplicates`: `=COUNTIF(Normalized!M:M,"duplicate_external_id")`
   - `unmapped_categories`: `=COUNTIF(Normalized!M:M,"category_missing")`

## 3) Правила форматирования

### `Normalized`
- Дубликаты `external_id` (A:A):
  - Custom formula: `=COUNTIF($A:$A,$A1)>1`
  - Цвет: красный фон.
- Расходы `amount < 0` (C:C): красный текст.
- Доходы `amount > 0` (C:C): зелёный текст.
- `is_refund = TRUE` (J:J): жёлтая подсветка строки.
- `notes_auto <> "ok"` (M:M): оранжевый фон.

### `Dashboard`
- Net < 0: красный.
- Net >= 0: зелёный.
- refund_total > 0: мягкая синяя подсветка (информативно).

## 4) Демо-набор данных (30+ normalized, включая refund)

Ниже — пример загрузки в `Normalized` (колонки A:G как source-of-truth):

| external_id | date | amount | currency | account | category | description |
|---|---|---:|---|---|---|---|
| cal-2026-01-0001 | 2026-01-03 | -210.45 | ILS | cal_visa_main | groceries | supermarket shufersal |
| cal-2026-01-0002 | 2026-01-03 | -34.90 | ILS | cal_visa_main | transport | bus rav-kav reload |
| cal-2026-01-0003 | 2026-01-04 | -58.00 | ILS | cal_visa_main | dining | coffee with team |
| cal-2026-01-0004 | 2026-01-05 | -420.00 | ILS | cal_visa_main | utilities | electricity bill |
| cal-2026-01-0005 | 2026-01-05 | 12500.00 | ILS | cal_visa_main | salary | salary january |
| cal-2026-01-0006 | 2026-01-06 | -49.90 | ILS | cal_visa_main | subscriptions | spotify annual |
| cal-2026-01-0007 | 2026-01-06 | -89.00 | ILS | cal_visa_main | healthcare | pharmacy purchase |
| cal-2026-01-0008 | 2026-01-07 | -312.00 | ILS | cal_visa_main | shopping | clothing store |
| cal-2026-01-0009 | 2026-01-08 | -120.00 | ILS | cal_visa_main | entertainment | cinema tickets |
| cal-2026-01-0010 | 2026-01-09 | -67.20 | ILS | cal_visa_main | dining | lunch order |
| cal-2026-01-0011 | 2026-01-09 | -29.90 | ILS | cal_visa_main | subscriptions | cloud storage |
| cal-2026-01-0012 | 2026-01-10 | -15.00 | ILS | cal_visa_main | fees | card monthly fee |
| cal-2026-01-0013 | 2026-01-11 | -540.00 | ILS | cal_visa_main | travel | train to haifa |
| cal-2026-01-0014 | 2026-01-12 | -180.00 | ILS | cal_visa_main | groceries | weekend market |
| cal-2026-01-0015 | 2026-01-12 | 35.00 | ILS | cal_visa_main | cashback | cashback campaign |
| cal-2026-01-0016 | 2026-01-13 | -96.50 | ILS | cal_visa_main | transport | fuel station |
| cal-2026-01-0017 | 2026-01-14 | -245.00 | ILS | cal_visa_main | shopping | electronics accessory |
| cal-2026-01-0018 | 2026-01-14 | 245.00 | ILS | cal_visa_main | refund | refund electronics accessory |
| cal-2026-01-0019 | 2026-01-15 | -72.00 | ILS | cal_visa_main | dining | dinner takeaway |
| cal-2026-01-0020 | 2026-01-16 | -410.00 | ILS | cal_visa_main | utilities | water and city tax |
| cal-2026-01-0021 | 2026-01-17 | -230.00 | ILS | cal_visa_main | groceries | supermarket mega |
| cal-2026-01-0022 | 2026-01-18 | -1500.00 | ILS | cal_visa_main | transfer | transfer to savings |
| cal-2026-01-0023 | 2026-01-19 | -84.30 | ILS | cal_visa_main | transport | taxi app |
| cal-2026-01-0024 | 2026-01-20 | -93.00 | ILS | cal_visa_main | healthcare | dentist visit |
| cal-2026-01-0025 | 2026-01-21 | -199.00 | ILS | cal_visa_main | shopping | home supplies |
| cal-2026-01-0026 | 2026-01-22 | -76.00 | ILS | cal_visa_main | entertainment | streaming rental |
| cal-2026-01-0027 | 2026-01-23 | -132.00 | ILS | cal_visa_main | groceries | fresh produce |
| cal-2026-01-0028 | 2026-01-24 | -58.50 | ILS | cal_visa_main | dining | lunch cafe |
| cal-2026-01-0029 | 2026-01-25 | -112.40 | ILS | cal_visa_main | transport | intercity bus |
| cal-2026-01-0030 | 2026-01-26 | -340.00 | ILS | cal_visa_main | travel | hotel booking partial |
| cal-2026-01-0031 | 2026-01-27 | 120.00 | ILS | cal_visa_main | refund | refund hotel overcharge |
| cal-2026-01-0032 | 2026-01-28 | -260.00 | ILS | cal_visa_main | utilities | internet provider |
| cal-2026-01-0033 | 2026-01-29 | -145.60 | ILS | cal_visa_main | groceries | late month grocery |
| cal-2026-01-0034 | 2026-01-30 | -63.00 | ILS | cal_visa_main | dining | office lunch |

## 5) Почему вариант `ledger_first`
- Наглядность: все ключевые движения в одном листе (`Normalized`).
- Масштабируемость: сводки строятся QUERY/SUMIFS без ручного ввода.
- Контроль ошибок: встроенные проверки duplicate/unmapped.
- Совместимость с авто-импортом: ручные поля не требуются.
