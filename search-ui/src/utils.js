const monthNames = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul","Aug", "Sep", "Oct", "Nov", "Dec"];
export const getMonthName = (e) => monthNames[e];

export const extractDate = (hit) => hit.id.split('.')[0].match(/.{2}/g).map(num => parseInt(num, 10));

export const unique = (a) => [...new Set(a)];

export const count = (array, extractor) => {
  const counts = {};
  array.map(extractor).flat().forEach(e => {
    if (e in counts) { counts[e] += 1 } else { counts[e] = 1 }
  });
  return counts;
};