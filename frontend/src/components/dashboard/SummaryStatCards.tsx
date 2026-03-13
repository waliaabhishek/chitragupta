import { Card, Col, Row, Skeleton, Statistic } from "antd";
import { formatCurrency } from "../../utils/aggregation";
import type { AggregationResponse } from "../../types/api";

interface SummaryStatCardsProps {
  data: AggregationResponse | null;
  isLoading: boolean;
  error?: string | null;
}

export function SummaryStatCards({ data, isLoading, error }: SummaryStatCardsProps): JSX.Element {
  const total = data ? parseFloat(data.total_amount) : 0;
  const usage = data ? parseFloat(data.usage_amount) : 0;
  const shared = data ? parseFloat(data.shared_amount) : 0;

  const cards = [
    { title: "Total Cost", value: total },
    { title: "Usage Cost", value: usage },
    { title: "Shared Cost", value: shared },
  ];

  return (
    <Row gutter={[16, 16]}>
      {cards.map(({ title, value }) => (
        <Col xs={24} sm={8} key={title}>
          <Card>
            {isLoading ? (
              <Skeleton active paragraph={false} />
            ) : (
              <Statistic title={title} value={error ? "—" : formatCurrency(value)} />
            )}
          </Card>
        </Col>
      ))}
    </Row>
  );
}
