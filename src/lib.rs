/// Marzullo's algorithm, invented by Keith Marzullo for his Ph.D. dissertation in 1984, is an
/// agreement algorithm used to select sources for estimating accurate time from a number of noisy
/// time sources. NTP uses a modified form of this called the Intersection algorithm, which returns
/// a larger interval for further statistical sampling. However, here we want the smallest interval.
/// Here is a description of the algorithm:
/// https://en.wikipedia.org/wiki/Marzullo%27s_algorithm#Method
/// This is a port of the TigerBeetle implementation done mainly by Joran Dirk Greef (https://github.com/jorangreef) and King Protty (https://github.com/kprotty):
/// see it here https://github.com/tigerbeetle/tigerbeetle/blob/main/src/vsr/marzullo

#[derive(Debug, Clone)]
pub struct Interval {
    lower_bound: i64,
    upper_bound: i64,
    sources_true: u8,
    sources_false: u8,
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum BoundType {
    Lower,
    Upper,
}

#[derive(Debug, Clone)]
pub struct SourceBound {
    value: i64,
    /// An identifier, the index of the clock source in the list of clock sources:
    source: u8,
    bound_type: BoundType,
}

impl PartialEq for SourceBound {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value && self.bound_type == other.bound_type
    }
}

impl Eq for SourceBound {}

impl PartialOrd for SourceBound {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// If two source bounds with the same value but opposite
/// bound types exist, indicating that one interval ends just as another begins, then a method of
/// deciding which comes first is necessary. Such an occurrence can be considered an overlap
/// with no duration, which can be found by the algorithm by sorting the lower bound before the
/// upper bound. Alternatively, if such pathological overlaps are considered objectionable then
/// they can be avoided by sorting the upper bound before the lower bound.
impl Ord for SourceBound {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self == other {
            // Use the source index to break the tie and ensure the sort is fully specified and stable
            // so that different sort algorithms sort the same way:
            if self.source < other.source {
                return std::cmp::Ordering::Less;
            }
            if self.source > other.source {
                return std::cmp::Ordering::Greater;
            }
            return std::cmp::Ordering::Equal;
        }

        if self.value < other.value {
            return std::cmp::Ordering::Less;
        }

        if self.value > other.value {
            return std::cmp::Ordering::Greater;
        }

        if self.bound_type == BoundType::Lower && other.bound_type == BoundType::Upper {
            return std::cmp::Ordering::Less;
        }

        if self.bound_type == BoundType::Upper && other.bound_type == BoundType::Lower {
            return std::cmp::Ordering::Greater;
        }

        unreachable!("inconceivable! unable to compare SourceBound structs.")
    }
}

#[derive(Debug)]
pub enum MarzulloError {
    InvalidSourceBounds(String),
    InvalidSourceBoundsOrder(String),
    IntervalInvariant(String),
}

impl std::fmt::Display for MarzulloError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            MarzulloError::InvalidSourceBounds(msg) => {
                write!(f, "Invalid source bounds order: {}", msg)
            }
            MarzulloError::InvalidSourceBoundsOrder(msg) => {
                write!(f, "Invalid source bounds sorting: {}", msg)
            }
            MarzulloError::IntervalInvariant(msg) => {
                write!(f, "Interval invariant : {}", msg)
            }
        }
    }
}

impl std::error::Error for MarzulloError {}

impl Interval {
    /// Returns the smallest interval consistent with the largest number of sources.
    pub fn try_from_source_bounds(
        source_bounds: Vec<SourceBound>,
    ) -> Result<Interval, MarzulloError> {
        // There are two bounds (lower and upper) per source.
        let sources = source_bounds.len() / 2;
        if sources == 0 {
            return Ok(Interval {
                lower_bound: 0,
                upper_bound: 0,
                sources_true: 0,
                sources_false: 0,
            });
        }

        let mut bounds = source_bounds.clone();
        bounds.sort();

        if !bounds
            .get(0)
            .is_some_and(|b| b.bound_type == BoundType::Lower)
        {
            return Err(MarzulloError::InvalidSourceBounds(
                "first bound should be a lower bound".to_string(),
            ));
        }

        let mut best = 0;
        let mut count = 0;
        let mut iter_prev_bound: Option<&SourceBound> = None;
        let mut interval: Option<Interval> = None;

        for (idx, bound) in bounds.iter().enumerate() {
            // Verify that our sort implementation is correct:
            if let Some(prevb) = iter_prev_bound {
                if prevb > bound {
                    return Err(MarzulloError::InvalidSourceBoundsOrder(format!(
                        "expected {:?} to be less than or equal to {:?}",
                        prevb, bound
                    )));
                }
            }

            iter_prev_bound = Some(bound);

            // Update the current number of overlapping intervals:
            match bound.bound_type {
                BoundType::Lower => count += 1,
                BoundType::Upper => count -= 1,
            }

            // The last upper bound tuple will have a count of one less than the lower bound.
            // Therefore, we should never see count >= best for the last tuple:
            if count > best && idx < bounds.len() - 1 {
                best = count;
                interval = Some(Interval {
                    lower_bound: bound.value,
                    upper_bound: bounds[idx + 1].value,
                    sources_true: 0,
                    sources_false: 0,
                });
            } else if count == best
                && idx < bounds.len() - 1
                && bounds[idx + 1].bound_type == BoundType::Upper
            {
                // This is a tie for best overlap. Both intervals have the same number of sources.
                // We want to choose the smaller of the two intervals:
                let alternative = bounds[idx + 1].value - bound.value;
                if let Some(ref ivl) = interval {
                    if alternative < ivl.upper_bound - ivl.lower_bound {
                        interval = Some(Interval {
                            lower_bound: bound.value,
                            upper_bound: bounds[idx + 1].value,
                            sources_true: 0,
                            sources_false: 0,
                        });
                    }
                }
            }
        }

        if !iter_prev_bound.is_some_and(|b| b.bound_type == BoundType::Upper) {
            return Err(MarzulloError::IntervalInvariant(
                "expected last visited source bound to be an upper bound.".to_string(),
            ));
        }

        if best > sources {
            return Err(MarzulloError::IntervalInvariant(
                format!( "best count of overlapping intervals should be less than or equal to the number of sources.
                best: {}, sources: {}", best, sources)
            ));
        }

        // The number of false sources (ones which do not overlap the optimal interval) is the
        // number of sources minus the value of `best`:
        interval = interval.map(|mut ivl| {
            ivl.sources_true = best as u8;
            ivl.sources_false = (sources - best) as u8;
            ivl
        });

        if !interval
            .as_ref()
            .is_some_and(|ivl| ivl.sources_true + ivl.sources_false == sources as u8)
        {
            return Err(MarzulloError::IntervalInvariant(
                "expected the sum of interval's sources_true and sources_false to be equal to the number of sources.".to_string()
            ));
        }

        match interval {
            Some(ivl) => Ok(ivl),
            _ => unreachable!(
                "this branch should never be reached since the computed interval was asserted to be valid."
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_bound_cmp() {
        let lower_bound = SourceBound {
            source: 1,
            value: 1,
            bound_type: BoundType::Lower,
        };
        let upper_bound = SourceBound {
            source: 1,
            value: 1,
            bound_type: BoundType::Upper,
        };
        assert!(lower_bound < upper_bound);

        let lower_bound = SourceBound {
            source: 1,
            value: 1,
            bound_type: BoundType::Lower,
        };

        let upper_bound = SourceBound {
            source: 2,
            value: 1,
            bound_type: BoundType::Upper,
        };
        assert!(lower_bound < upper_bound);

        let lower_bound = SourceBound {
            source: 1,
            value: 1,
            bound_type: BoundType::Lower,
        };

        let upper_bound = SourceBound {
            source: 1,
            value: 2,
            bound_type: BoundType::Upper,
        };

        assert!(lower_bound < upper_bound);
    }

    fn source_bounds_generator(seed: Vec<i64>) -> Vec<SourceBound> {
        let mut source_bounds = Vec::new();
        for (idx, value) in seed.iter().enumerate() {
            let bound_type = if idx % 2 == 0 {
                BoundType::Lower
            } else {
                BoundType::Upper
            };
            source_bounds.push(SourceBound {
                source: (idx as u8) / 2,
                value: *value,
                bound_type,
            });
        }
        source_bounds
    }

    #[test]
    fn test_marzullo_interval_from_source_bounds() {
        let source_bounds = source_bounds_generator(vec![11, 13, 10, 12, 8, 12]);
        let interval = Interval::try_from_source_bounds(source_bounds).unwrap();
        assert_eq!(interval.lower_bound, 11);
        assert_eq!(interval.upper_bound, 12);
        assert_eq!(interval.sources_true, 3);
        assert_eq!(interval.sources_false, 0);

        let source_bounds = source_bounds_generator(vec![8, 12, 11, 13, 14, 15]);
        let interval = Interval::try_from_source_bounds(source_bounds).unwrap();
        assert_eq!(interval.lower_bound, 11);
        assert_eq!(interval.upper_bound, 12);
        assert_eq!(interval.sources_true, 2);
        assert_eq!(interval.sources_false, 1);

        let source_bounds = source_bounds_generator(vec![-10, 10, -1, 1, 0, 0]);
        let interval = Interval::try_from_source_bounds(source_bounds).unwrap();
        assert_eq!(interval.lower_bound, 0);
        assert_eq!(interval.upper_bound, 0);
        assert_eq!(interval.sources_true, 3);
        assert_eq!(interval.sources_false, 0);

        // The upper bound of the first interval overlaps inclusively with the lower of the last.
        let source_bounds = source_bounds_generator(vec![8, 12, 10, 11, 8, 10]);
        let interval = Interval::try_from_source_bounds(source_bounds).unwrap();
        assert_eq!(interval.lower_bound, 10);
        assert_eq!(interval.upper_bound, 10);
        assert_eq!(interval.sources_true, 3);
        assert_eq!(interval.sources_false, 0);

        // The first smallest interval is selected. The alternative with equal overlap is 10..12.
        // However, while this shares the same number of sources, it is not the smallest interval.
        let source_bounds = source_bounds_generator(vec![8, 12, 10, 12, 8, 9]);
        let interval = Interval::try_from_source_bounds(source_bounds).unwrap();
        assert_eq!(interval.lower_bound, 8);
        assert_eq!(interval.upper_bound, 9);
        assert_eq!(interval.sources_true, 2);
        assert_eq!(interval.sources_false, 1);

        // The last smallest interval is selected. The alternative with equal overlap is 7..9.
        // However, while this shares the same number of sources, it is not the smallest interval.
        let source_bounds = source_bounds_generator(vec![7, 9, 7, 12, 10, 11]);
        let interval = Interval::try_from_source_bounds(source_bounds).unwrap();
        assert_eq!(interval.lower_bound, 10);
        assert_eq!(interval.upper_bound, 11);
        assert_eq!(interval.sources_true, 2);
        assert_eq!(interval.sources_false, 1);

        // The same idea as the previous test, but with negative offsets.
        let source_bounds = source_bounds_generator(vec![-9, -7, -12, -7, -11, -10]);
        let interval = Interval::try_from_source_bounds(source_bounds).unwrap();
        assert_eq!(interval.lower_bound, -11);
        assert_eq!(interval.upper_bound, -10);
        assert_eq!(interval.sources_true, 2);
        assert_eq!(interval.sources_false, 1);

        // A cluster of one with no remote sources.
        let source_bounds = source_bounds_generator(vec![]);
        let interval = Interval::try_from_source_bounds(source_bounds).unwrap();
        assert_eq!(interval.lower_bound, 0);
        assert_eq!(interval.upper_bound, 0);
        assert_eq!(interval.sources_true, 0);
        assert_eq!(interval.sources_false, 0);

        // A cluster of two with one remote source.
        let source_bounds = source_bounds_generator(vec![1, 3]);
        let interval = Interval::try_from_source_bounds(source_bounds).unwrap();
        assert_eq!(interval.lower_bound, 1);
        assert_eq!(interval.upper_bound, 3);
        assert_eq!(interval.sources_true, 1);
        assert_eq!(interval.sources_false, 0);

        // A cluster of three with agreement.
        let source_bounds = source_bounds_generator(vec![1, 3, 2, 2]);
        let interval = Interval::try_from_source_bounds(source_bounds).unwrap();
        assert_eq!(interval.lower_bound, 2);
        assert_eq!(interval.upper_bound, 2);
        assert_eq!(interval.sources_true, 2);
        assert_eq!(interval.sources_false, 0);

        // A cluster of three with agreement.
        let source_bounds = source_bounds_generator(vec![1, 3, 4, 5]);
        let interval = Interval::try_from_source_bounds(source_bounds).unwrap();
        assert_eq!(interval.lower_bound, 4);
        assert_eq!(interval.upper_bound, 5);
        assert_eq!(interval.sources_true, 1);
        assert_eq!(interval.sources_false, 1);
    }
}
