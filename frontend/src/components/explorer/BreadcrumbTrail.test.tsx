import { fireEvent, render } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { BreadcrumbTrail } from "./BreadcrumbTrail";

const DEFAULT_HANDLERS = {
  onNavigate: vi.fn(),
  onGoBack: vi.fn(),
  onGoToRoot: vi.fn(),
};

describe("BreadcrumbTrail", () => {
  it("renders 'Tenant' as root breadcrumb label", () => {
    const { getByText } = render(
      <BreadcrumbTrail breadcrumbs={[]} {...DEFAULT_HANDLERS} />,
    );
    expect(getByText("Tenant")).toBeInTheDocument();
  });

  it("renders each breadcrumb item with correct label", () => {
    const breadcrumbs = [
      { id: "env-abc", label: "my-env", type: "environment" },
      { id: "lkc-abc", label: "my-cluster", type: "kafka_cluster" },
    ];
    const { getByText } = render(
      <BreadcrumbTrail breadcrumbs={breadcrumbs} {...DEFAULT_HANDLERS} />,
    );
    expect(getByText("my-env")).toBeInTheDocument();
    expect(getByText("my-cluster")).toBeInTheDocument();
  });

  it("calls onGoToRoot when root breadcrumb is clicked", () => {
    const onGoToRoot = vi.fn();
    const { getAllByRole } = render(
      <BreadcrumbTrail
        breadcrumbs={[]}
        onGoToRoot={onGoToRoot}
        onNavigate={vi.fn()}
        onGoBack={vi.fn()}
      />,
    );
    // Root home button is always the first button
    fireEvent.click(getAllByRole("button")[0]);
    expect(onGoToRoot).toHaveBeenCalledTimes(1);
  });

  it("calls onNavigate(index) when intermediate breadcrumb is clicked", () => {
    const onNavigate = vi.fn();
    const breadcrumbs = [
      { id: "env-abc", label: "my-env", type: "environment" },
      { id: "lkc-abc", label: "my-cluster", type: "kafka_cluster" },
    ];
    const { getByText } = render(
      <BreadcrumbTrail
        breadcrumbs={breadcrumbs}
        onNavigate={onNavigate}
        onGoBack={vi.fn()}
        onGoToRoot={vi.fn()}
      />,
    );
    fireEvent.click(getByText("my-env"));
    expect(onNavigate).toHaveBeenCalledWith(0);
  });

  it("current (last) breadcrumb does not trigger navigation when clicked", () => {
    const onNavigate = vi.fn();
    const breadcrumbs = [
      { id: "env-abc", label: "my-env", type: "environment" },
      { id: "lkc-abc", label: "my-cluster", type: "kafka_cluster" },
    ];
    const { getByText } = render(
      <BreadcrumbTrail
        breadcrumbs={breadcrumbs}
        onNavigate={onNavigate}
        onGoBack={vi.fn()}
        onGoToRoot={vi.fn()}
      />,
    );
    // Last breadcrumb should not fire onNavigate (rendered as non-interactive text)
    fireEvent.click(getByText("my-cluster"));
    expect(onNavigate).not.toHaveBeenCalled();
  });

  it("does not show Back button when breadcrumbs is empty", () => {
    const { queryByText } = render(
      <BreadcrumbTrail breadcrumbs={[]} {...DEFAULT_HANDLERS} />,
    );
    expect(queryByText("←")).toBeNull();
  });

  it("shows Back button when there are breadcrumbs", () => {
    const breadcrumbs = [
      { id: "env-abc", label: "my-env", type: "environment" },
    ];
    const { getByText } = render(
      <BreadcrumbTrail breadcrumbs={breadcrumbs} {...DEFAULT_HANDLERS} />,
    );
    expect(getByText("←")).toBeInTheDocument();
  });

  it("calls onGoBack when Back button is clicked", () => {
    const onGoBack = vi.fn();
    const breadcrumbs = [
      { id: "env-abc", label: "my-env", type: "environment" },
    ];
    const { getByText } = render(
      <BreadcrumbTrail
        breadcrumbs={breadcrumbs}
        onGoBack={onGoBack}
        onNavigate={vi.fn()}
        onGoToRoot={vi.fn()}
      />,
    );
    fireEvent.click(getByText("←"));
    expect(onGoBack).toHaveBeenCalledTimes(1);
  });
});
